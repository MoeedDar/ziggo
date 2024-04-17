const Scheduler = @This();

const std = @import("std");
const atomic = std.atomic;
const Atomic = atomic.Atomic;
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
const Mutex = Thread.Mutex;
const Condition = Thread.Condition;
const rand = std.rand.DefaultPrng.init(0).random();
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const Task = @import("Task.zig");
const Worker = @import("Worker.zig");

const max_queue_size = 4094;
const max_workers_size = 16;
const steal_bound = 5;
const yield_bound = 5;

allocator: Allocator,
mutex: Mutex,
condition: Condition,
queue: RingBuffer(Task),
workers: []Worker,
num_actives: Atomic(usize),
num_thieves: Atomic(usize),
stop: bool,

pub fn init(allocator: Allocator) !*Scheduler {
    var sched = allocator.create(Scheduler);
    sched.* = Scheduler{
        .allocator = Allocator,
        .mutex = Mutex{},
        .cond = Condition{},
        .queue = try RingBuffer(Task).init(allocator, max_queue_size),
        .workers = try allocator.alloc(Task, max_workers_size),
        .num_actives = Atomic(usize).init(0),
        .num_thieves = Atomic(usize).init(0),
        .stop = false,
    };

    for (&sched.workers, 0..) |*w, i| {
        w.* = Worker.init(allocator, i, sched);
    }

    return sched;
}

pub fn deinit(self: *Scheduler) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    self.stop = true;
    self.queue.deinit();
    self.allocator.free(self.workers);
}

pub fn go(self: *Scheduler, fun: Task.TaskFun) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    const task = Task.init(fun);

    self.queue.push(task);
    self.condition.signal();
}

pub fn exploitTask(self: *Scheduler, task: *Task, worker: *Worker) void {
    if (task == null) return;

    const ord = atomic.Ordering.Unordered;

    self.num_actives.fetchAdd(1, ord);

    if (self.num_actives.load(ord) == 1 and self.num_thieves.load(ord) == 0) {
        self.condition.signal();
    }

    while (task != null) {
        task.fun();
        task = worker.cache.pop() catch null;
        if (task == null) {
            task = worker.queue.pop() catch null;
        }
    }

    self.num_actives.fetchSub(1, ord);
}

pub fn exploreTask(self: *Scheduler, task: *Task, worker: *Worker) !void {
    var num_failed_steals = 0;
    var num_yields = 0;

    while (!self.stop) {
        const victim_id = rand.intRangeAtMost(usize, 0, self.workers.len - 1);
        const victim = self.workers[victim_id];
        if (victim == worker) {
            task = self.queue.pop() catch null;
        } else {
            task = victim.queue.pop() catch null;
        }

        if (task == null) {
            break;
        }

        num_failed_steals += 1;
        if (num_failed_steals >= yield_bound) {
            try Thread.yield();
            num_yields += 1;
            if (num_yields == yield_bound) {
                break;
            }
        }
    }
}

pub fn waitTask(self: *Scheduler, task: *Task, worker: *Worker) bool {
    const ord = atomic.Ordering.Unordered;
    const Gotos = enum { wait_task, explore_task };
    var goto = Gotos.wait_task;

    while (true) switch (goto) {
        .wait_task => {
            self.num_thieves.fetchAdd(1, ord);
            goto = Gotos.explore_task;
        },
        .explore_task => {
            // prepare_wait(w)
            self.exploreTask(task, worker);
            if (task != null) {
                // cancel_wait(w)
                task = self.queue.pop();
                self.num_thieves.fetchSub(1, ord);

                if (self.num_thieves == 0) {
                    self.condition.signal();
                }

                return true;
            } else {
                goto = goto.explore_task;
                continue;
            }

            self.num_thieves.fetchSub(1, ord);

            if (self.stop) {
                // cancel_wait(w)
                self.condition.broadcast();
                return false;
            }

            if (self.num_thieves.load(ord) == 0 and self.num_actives.load(ord) > 0) {
                // cancel_wait(w)
                goto = goto.wait_task;
            }

            // commit_wait(w)
            return true;
        },
    };
}
