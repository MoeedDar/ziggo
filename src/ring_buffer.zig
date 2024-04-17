const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

pub fn RingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        const Error = error{
            RingBufferOverflow,
            RingBufferUnderflow,
        };

        allocator: Allocator,
        data: []T,
        head: usize,
        tail: usize,
        count: usize,

        pub fn init(allocator: Allocator, size: usize) !RingBuffer(T) {
            return RingBuffer(T){
                .allocator = allocator,
                .buf = try allocator.alloc(T, size),
                .head = 0,
                .tail = 0,
                .count = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.data);
        }

        pub fn isFull(self: *Self) bool {
            return self.count == self.data.len;
        }

        pub fn isEmpty(self: *Self) bool {
            return self.count == 0;
        }

        pub fn push(self: *Self, value: T) !void {
            if (self.isFull()) {
                return Error.RingBufferOverflow;
            }

            self.data[self.head] = value;
            self.head = (self.head + 1) % self.data.len;
            self.count += 1;
        }

        pub fn pop(self: *Self) !T {
            if (self.isEmpty()) {
                return Error.RingBufferUnderflow;
            }

            const value = self.data[self.tail];

            self.tail = (self.tail + 1) % self.data.len;
            self.count -= 1;

            return value;
        }
    };
}

test "ring buffer test" {
    var allocator = testing.allocator;
    const size: usize = 5;

    var buffer = try RingBuffer(usize).init(allocator, size);
    defer buffer.deinit();

    for (0..size) |i| {
        try buffer.push(i);
    }

    try testing.expectError(RingBuffer(usize).Error.RingBufferOverflow, buffer.push(0));

    for (0..size) |i| {
        const value = try buffer.pop();
        try testing.expect(value == i);
    }

    try testing.expectError(RingBuffer(usize).Error.RingBufferUnderflow, buffer.pop());
}
