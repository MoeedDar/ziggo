const Worker = @This();

const std = @import("std");
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
const Scheduler = @import("Scheduler.zig");
const RingBuffer = @import("ring_buffer.zig").RingBuffer;
const Task = @import("Task.zig");

const max_worker_queue_size = 4096;

allocator: Allocator,
id: usize,
scheduler: *Scheduler,
queue: RingBuffer(Task),
cache: RingBuffer(Task),

pub fn init(allocator: Allocator, id: usize, scheduler: Scheduler) !Worker {
    const worker = Worker{
        .allocator = allocator,
        .id = id,
        .scheduler = scheduler,
        .queue = try RingBuffer(Task).init(allocator, max_worker_queue_size),
        .cache = try RingBuffer(Task).init(allocator, max_worker_queue_size),
    };
    try Thread.spawn(.{}, run, .{worker});
    return worker;
}

pub fn deinit(self: *Worker) void {
    self.queue.deinit();
    self.cache.deinit();
}

fn run(self: *Worker) void {
    var task: *Task = null;
    while (!self.scheduler.stop) {
        task = self.scheduler.exploitTask(task, self);
        if (!self.scheduler.waitTask(task, self)) {
            break;
        }
    }
}
