const Task = @This();

pub const TaskFun = *const fn () void;

fun: TaskFun,

pub fn init(fun: TaskFun) Task {
    return Task{
        .fun = fun,
    };
}
