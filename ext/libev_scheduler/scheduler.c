#include <netdb.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdnoreturn.h>

#include "../libev/ev.h"
#include "ruby.h"
#include "ruby/io.h"
#include "../libev/ev.h"

// Some debugging facilities
#define INSPECT(str, obj) { \
  printf(str); \
  VALUE s = rb_funcall(obj, rb_intern("inspect"), 0); \
  printf(": %s\n", StringValueCStr(s)); \
}
#define TRACE_CALLER() { \
  VALUE c = rb_funcall(rb_mKernel, rb_intern("caller"), 0); \
  INSPECT("caller: ", c); \
}

ID ID_ivar_is_nonblocking;
ID ID_ivar_io;
VALUE VALUE_nil;

// IO event mask (from IO::READABLE & IO::WRITEABLE)
int event_readable;
int event_writable;

typedef struct Scheduler_t {
  struct ev_loop *ev_loop;
  struct ev_async break_async; // used for breaking out of blocking event loop

  unsigned int pending_count;
  unsigned int currently_polling;
  VALUE ready_fibers;
} Scheduler_t;

static size_t Scheduler_size(const void *ptr) {
  return sizeof(Scheduler_t);
}

static void Scheduler_mark(void *ptr) {
  Scheduler_t *scheduler = ptr;
  rb_gc_mark(scheduler->ready_fibers);
}

static const rb_data_type_t Scheduler_type = {
    "LibevScheduler",
    {Scheduler_mark, 0, Scheduler_size,},
    0, 0, RUBY_TYPED_FREE_IMMEDIATELY
};

static VALUE Scheduler_allocate(VALUE klass) {
  Scheduler_t *scheduler = ALLOC(Scheduler_t);

  return TypedData_Wrap_Struct(klass, &Scheduler_type, scheduler);
}

#define GetScheduler(obj, scheduler) \
  TypedData_Get_Struct((obj), Scheduler_t, &Scheduler_type, (scheduler))

void break_async_callback(struct ev_loop *ev_loop, struct ev_async *ev_async, int revents) {
  // This callback does nothing, the break async is used solely for breaking out
  // of a *blocking* event loop (waking it up) in a thread-safe, signal-safe manner
}

static VALUE Scheduler_initialize(VALUE self) {
  Scheduler_t *scheduler;

  GetScheduler(self, scheduler);
  // Always use a private event loop, even in the main thread. Using
  // EV_DEFAULT (the process-wide default libev loop) would share it with
  // any other library that also drives EV_DEFAULT in the main thread (e.g.
  // Polyphony's own native backend, which uses EV_DEFAULT for its main
  // thread as well). When two independent fiber-scheduling systems drive
  // the same shared loop, whichever one calls ev_run first will
  // synchronously fire the *other* system's due watchers too, resuming
  // fibers it doesn't own via the wrong resume/transfer convention and
  // corrupting their continuation state (manifesting as "attempt to
  // yield/resume a(n) (not resumed|transferring) fiber" FiberErrors). A
  // private loop guarantees this scheduler never processes -- or is
  // interfered with by -- watchers belonging to another library.
  scheduler->ev_loop = ev_loop_new(EVFLAG_NOSIGMASK);

  ev_async_init(&scheduler->break_async, break_async_callback);
  ev_async_start(scheduler->ev_loop, &scheduler->break_async);
  ev_unref(scheduler->ev_loop); // don't count the break_async watcher

  scheduler->pending_count = 0;
  scheduler->currently_polling = 0;
  scheduler->ready_fibers = rb_ary_new();

  return Qnil;
}

VALUE Scheduler_poll(VALUE self);

VALUE Scheduler_run(VALUE self) {
  Scheduler_t *scheduler;
  GetScheduler(self, scheduler);

  while (scheduler->pending_count > 0 || RARRAY_LEN(scheduler->ready_fibers) > 0) {
    Scheduler_poll(self);
  }

  return self;
}

VALUE Scheduler_close(VALUE self) {
  Scheduler_t *scheduler;
  GetScheduler(self, scheduler);

  Scheduler_run(self);

  ev_async_stop(scheduler->ev_loop, &scheduler->break_async);
  if (!ev_is_default_loop(scheduler->ev_loop)) ev_loop_destroy(scheduler->ev_loop);
  return self;
}

struct libev_timer {
  struct ev_timer timer;
  Scheduler_t *scheduler;
  VALUE fiber;
};

#define SCHEDULE(scheduler, fiber) SCHEDULE_VALUE((scheduler), (fiber), Qnil)
#define SCHEDULE_VALUE(scheduler, fiber, value) \
  rb_ary_push((scheduler)->ready_fibers, rb_ary_new_from_args(2, (fiber), (value)))

void Scheduler_timer_callback(EV_P_ ev_timer *w, int revents) {
  struct libev_timer *watcher = (struct libev_timer *)w;
  SCHEDULE(watcher->scheduler, watcher->fiber);
}

VALUE rb_fiber_yield_value(VALUE _value) {
  return rb_fiber_yield(1, &VALUE_nil);
}

VALUE rb_fiber_yield_rescue(VALUE _value, VALUE err) {
  return err;
}

#define YIELD() rb_rescue2(rb_fiber_yield_value, VALUE_nil, rb_fiber_yield_rescue, VALUE_nil, rb_eException)

VALUE Scheduler_sleep(VALUE self, VALUE duration) {
  Scheduler_t *scheduler;
  struct libev_timer watcher;
  GetScheduler(self, scheduler);

  watcher.scheduler = scheduler;
  watcher.fiber = rb_fiber_current();
  ev_timer_init(&watcher.timer, Scheduler_timer_callback, NUM2DBL(duration), 0.);
  ev_timer_start(scheduler->ev_loop, &watcher.timer);
  scheduler->pending_count++;
  VALUE ret = YIELD();
  scheduler->pending_count--;
  ev_timer_stop(scheduler->ev_loop, &watcher.timer);
  if (!NIL_P(ret)) rb_exc_raise(ret);
  RB_GC_GUARD(watcher.fiber);
  RB_GC_GUARD(ret);
  return ret;
}

VALUE Scheduler_pause(VALUE self) {
  Scheduler_t *scheduler;
  GetScheduler(self, scheduler);

  ev_ref(scheduler->ev_loop);
  scheduler->pending_count++;
  VALUE ret = YIELD();
  scheduler->pending_count--;
  ev_unref(scheduler->ev_loop);
  if (!NIL_P(ret)) rb_exc_raise(ret);
  return ret;
}

VALUE Scheduler_block(int argc, VALUE *argv, VALUE self) {
  VALUE timeout = (argc == 2) ? argv[1] : Qnil;
  if (timeout != Qnil)
    Scheduler_sleep(self, timeout);
  else
    Scheduler_pause(self);
  
  return Qtrue;
}

VALUE Scheduler_unblock(VALUE self, VALUE blocker, VALUE fiber) {
  Scheduler_t *scheduler;
  GetScheduler(self, scheduler);

  SCHEDULE(scheduler, fiber);

  if (scheduler->currently_polling)
    ev_async_send(scheduler->ev_loop, &scheduler->break_async);

  return self;
}

// Interrupts a fiber that is currently blocked in the scheduler (e.g. in
// Scheduler_sleep, Scheduler_pause, Scheduler_io_wait or
// Scheduler_process_wait), causing it to resume with the given exception
// raised. This is the `fiber_interrupt` Fiber::Scheduler hook, required since
// Ruby 4.0 for safely delivering interrupts (e.g. signals such as SIGINT) to
// fibers blocked on scheduler-managed operations.
//
// This queues the interrupt exactly like a normal wakeup (via
// SCHEDULE_VALUE), so it is delivered later by Scheduler_resume_ready via
// rb_fiber_resume -- the *same* resume call that would otherwise wake this
// fiber normally. This matters because these fibers are suspended via
// rb_fiber_yield (a resume/yield-paired continuation, not a symmetric
// transfer): calling Fiber#raise (or Fiber#transfer) directly on such a
// fiber does not pair correctly with rb_fiber_yield and corrupts the
// fiber's resume chain (its `prev` pointer), causing a subsequent, unrelated
// Fiber.yield call within that same fiber to fail with "attempt to yield on
// a not resumed fiber". Resuming with the exception as the resume value,
// which Scheduler_sleep/Scheduler_pause explicitly detect and re-raise via
// rb_exc_raise(ret), avoids this corruption entirely.
VALUE Scheduler_fiber_interrupt(VALUE self, VALUE fiber, VALUE exception) {
  Scheduler_t *scheduler;
  GetScheduler(self, scheduler);

  SCHEDULE_VALUE(scheduler, fiber, exception);

  if (scheduler->currently_polling)
    ev_async_send(scheduler->ev_loop, &scheduler->break_async);

  return self;
}

struct libev_io {
  struct ev_io io;
  Scheduler_t *scheduler;
  VALUE fiber;
};

void Scheduler_io_callback(EV_P_ ev_io *w, int revents)
{
  struct libev_io *watcher = (struct libev_io *)w;
  SCHEDULE(watcher->scheduler, watcher->fiber);
}

int io_event_mask(VALUE events) {
  int interest = NUM2INT(events);
  int mask = 0;
  if (interest & event_readable) mask |= EV_READ;
  if (interest & event_writable) mask |= EV_WRITE;
  return mask;
}

VALUE Scheduler_io_wait(VALUE self, VALUE io, VALUE events, VALUE timeout) {
  Scheduler_t *scheduler;
  struct libev_io io_watcher;
  struct libev_timer timeout_watcher;
  GetScheduler(self, scheduler);

  VALUE underlying_io = rb_ivar_get(io, ID_ivar_io);
  if (underlying_io != Qnil) io = underlying_io;

#ifdef HAVE_RB_IO_DESCRIPTOR
  int fd = rb_io_descriptor(io);
#else
  rb_io_t *fptr;
  GetOpenFile(io, fptr);
  int fd = fptr->fd;
#endif

  io_watcher.scheduler = scheduler;
  io_watcher.fiber = rb_fiber_current();
  ev_io_init(&io_watcher.io, Scheduler_io_callback, fd, io_event_mask(events));

  int use_timeout = timeout != Qnil;
  if (use_timeout) {
    timeout_watcher.scheduler = scheduler;
    timeout_watcher.fiber = rb_fiber_current();
    ev_timer_init(&timeout_watcher.timer, Scheduler_timer_callback, NUM2DBL(timeout), 0.);
    ev_timer_start(scheduler->ev_loop, &timeout_watcher.timer);
  }

  ev_io_start(scheduler->ev_loop, &io_watcher.io);
  scheduler->pending_count++;
  VALUE ret = YIELD();
  scheduler->pending_count--;
  ev_io_stop(scheduler->ev_loop, &io_watcher.io);
  if (use_timeout)
    ev_timer_stop(scheduler->ev_loop, &timeout_watcher.timer);

  if (!NIL_P(ret)) rb_exc_raise(ret);

  if (use_timeout && ev_timer_remaining(scheduler->ev_loop, &timeout_watcher.timer) <= 0)
    return VALUE_nil;

  return events;
}

struct libev_child {
  struct ev_child child;
  Scheduler_t *scheduler;
  VALUE fiber;
  VALUE status;
};

void Scheduler_child_callback(EV_P_ ev_child *w, int revents)
{
  struct libev_child *watcher = (struct libev_child *)w;
  int exit_status = WEXITSTATUS(w->rstatus);
  watcher->status = rb_ary_new_from_args(2, INT2NUM(w->rpid), INT2NUM(exit_status));
  SCHEDULE(watcher->scheduler, watcher->fiber);
}

VALUE Scheduler_process_wait(VALUE self, VALUE pid, VALUE flags) {
  Scheduler_t *scheduler;
  struct libev_child watcher;
  VALUE result = Qnil;
  GetScheduler(self, scheduler);

  watcher.scheduler = scheduler;
  watcher.fiber = rb_fiber_current();
  watcher.status = Qnil;
  ev_child_init(&watcher.child, Scheduler_child_callback, NUM2INT(pid), 0);
  ev_child_start(scheduler->ev_loop, &watcher.child);
  scheduler->pending_count++;
  rb_fiber_yield(1, &VALUE_nil);
  scheduler->pending_count--;
  ev_child_stop(scheduler->ev_loop, &watcher.child);
  RB_GC_GUARD(watcher.status);
  RB_GC_GUARD(result);
  return result;
}

void Scheduler_resume_ready(Scheduler_t *scheduler) {
  VALUE ready_fibers = Qnil;

  unsigned int ready_count = RARRAY_LEN(scheduler->ready_fibers);
  while (ready_count > 0) {
    ready_fibers = scheduler->ready_fibers;
    scheduler->ready_fibers = rb_ary_new();

    for (unsigned int i = 0; i < ready_count; i++) {
      VALUE entry = RARRAY_AREF(ready_fibers, i);
      VALUE fiber = RARRAY_AREF(entry, 0);
      VALUE value = RARRAY_AREF(entry, 1);
      rb_fiber_resume(fiber, 1, &value);
    }

    ready_count = RARRAY_LEN(scheduler->ready_fibers);
  }

  RB_GC_GUARD(ready_fibers);
}

VALUE Scheduler_poll(VALUE self) {
  Scheduler_t *scheduler;
  GetScheduler(self, scheduler);

  scheduler->currently_polling = 1;
  ev_run(scheduler->ev_loop, EVRUN_ONCE);
  scheduler->currently_polling = 0;

  Scheduler_resume_ready(scheduler);

  return self;
}

VALUE Scheduler_pending_count(VALUE self) {
  Scheduler_t *scheduler;
  GetScheduler(self, scheduler);

  return INT2NUM(scheduler->pending_count);
}

void Init_Scheduler() {
  ev_set_allocator(xrealloc);

  VALUE mLibev = rb_define_module("Libev");
  VALUE cScheduler = rb_define_class_under(mLibev, "Scheduler", rb_cObject);
  rb_define_alloc_func(cScheduler, Scheduler_allocate);

  rb_define_method(cScheduler, "initialize", Scheduler_initialize, 0);

  // fiber scheduler interface
  rb_define_method(cScheduler, "close", Scheduler_close, 0);
  rb_define_method(cScheduler, "io_wait", Scheduler_io_wait, 3);
  rb_define_method(cScheduler, "process_wait", Scheduler_process_wait, 2);
  rb_define_method(cScheduler, "block", Scheduler_block, -1);
  rb_define_method(cScheduler, "unblock", Scheduler_unblock, 2);
  rb_define_method(cScheduler, "fiber_interrupt", Scheduler_fiber_interrupt, 2);

  rb_define_method(cScheduler, "run", Scheduler_run, 0);
  rb_define_method(cScheduler, "pending_count", Scheduler_pending_count, 0);

  ID_ivar_is_nonblocking = rb_intern("@is_nonblocking");
  ID_ivar_io             = rb_intern("@io");
  VALUE_nil              = Qnil;
  rb_global_variable(&VALUE_nil);

  event_readable = NUM2INT(rb_const_get(rb_cIO, rb_intern("READABLE")));
  event_writable = NUM2INT(rb_const_get(rb_cIO, rb_intern("WRITABLE")));
}
