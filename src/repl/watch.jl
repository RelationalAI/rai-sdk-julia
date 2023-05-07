# Generic file watching utlities

using Select, FileWatching

mutable struct CallbackLoop
    task::Task
    done::Channel{Nothing}
    isdone::Bool
end

function CallbackLoop(func, callback)
    done = Channel{Nothing}()
    task = @async begin
        while true
            @select begin
                (@async func()) |> e => callback(e)
                done => break
            end
        end
    end
    return CallbackLoop(task, done, false)
end

function stop!(cb::CallbackLoop)
    cb.isdone && return
    put!(cb.done, nothing)
    wait(cb.task)
    cb.isdone = true
    return
end

struct FileWatcher
    cb::CallbackLoop
end

FileWatcher(callback, path::AbstractString) =
    FileWatcher(CallbackLoop(() -> watch_file(path), callback))

stop!(fw::FileWatcher) = stop!(fw.cb)

struct DirWatcher
    dir::AbstractString
    cb::CallbackLoop
end

DirWatcher(callback, path::AbstractString) =
    DirWatcher(path, CallbackLoop(() -> watch_folder(path), callback))

function stop!(dw::DirWatcher)
    stop!(dw.cb)
    unwatch_folder(dw.dir)
    return
end

struct RecursiveWatcher
    root::AbstractString
    watcher::DirWatcher
    children::Dict{String,RecursiveWatcher}
end

function RecursiveWatcher(callback, path::AbstractString, root::AbstractString = "")
    children = Dict{String,RecursiveWatcher}()
    for f in readdir(path)
        child = joinpath(path, f)
        if isdir(child)
            children[f] = RecursiveWatcher(callback, child, joinpath(root, f))
        end
    end
    watcher = DirWatcher(path) do e
        f, event = e
        watched = haskey(children, f)
        dir = isdir(joinpath(path, f))
        if watched || dir
            if watched && !dir
                stop!(children[f])
                delete!(children, f)
            elseif dir && !watched
                children[f] = RecursiveWatcher(callback, joinpath(path, f), joinpath(root, f))
            end
        else
            callback(joinpath(root, f) => event)
        end
    end
    return RecursiveWatcher(root, watcher, children)
end

function stop!(rw::RecursiveWatcher)
    stop!(rw.watcher)
    foreach(stop!, values(rw.children))
    return
end
