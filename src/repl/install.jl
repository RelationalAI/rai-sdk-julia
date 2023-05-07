function install_source(conn, name, src)
    load_model(conn.ctx, conn.db, conn.engine, Dict(name => src))
end

function relfiles(dir, files = String[])
    for f in readdir(dir)
        path = joinpath(dir, f)
        if isdir(path)
            relfiles(path, files)
        elseif endswith(path, ".rel") && isfile(path)
            push!(files, path)
        end
    end
    return files
end

function install_folder(path)
    for file in relfiles(path)
        install_source(conn, abspath(file), String(read(file)))
    end
end

const watchers = Dict{String,Union{FileWatcher,RecursiveWatcher}}()

function is_watched_src(src)
    for (path, _) in watchers
        startswith(src, path) && return true
    end
    return false
end

function updatefile(src)
    if isfile(src)
        install_source(conn, src, String(read(src)))
    else
        delete_source(conn, src)
    end
end

function install(src)
    src = abspath(src)
    haskey(watchers, src) && return
    if isfile(src)
        install_source(conn, src, String(read(src)))
        watchers[src] = FileWatcher(src) do e
            updatefile(src)
        end
    elseif isdir(src)
        install_folder(src)
        watchers[src] = RecursiveWatcher(src) do e
            file, _ = e
            file = abspath(joinpath(src, file))
            endswith(file, ".rel") || return
            updatefile(file)
        end
    else
        printerr("path doesn't exist: $(src)")
    end
    return
end

function reset_sources()
    for (src, _) in list_source(conn)
        is_watched_src(src) && delete_source(conn, src)
    end
    foreach(stop!, values(watchers))
    empty!(watchers)
    return
end

function list_sources()
    ss = list_source(conn)
    for (name, src) in ss
        is_watched_src(name) || println(hidepwd(name))
    end
    for (src, _) in watchers
        println(hidepwd(src))
    end
end

function clear_db()
    # If we replace the global connection, the DB gets reset on every subsequent
    # query. So just create a temporary overwriting connection, and make a
    # single dummy query to carry out the reset.
    tmp = LocalConnection(dbname = conn.dbname, default_open_mode = CREATE_OVERWRITE)
    query(tmp, "")
end
