module REPLMode

using Markdown
using ..RAI
# using RelationalAIProtocol: OPEN_OR_CREATE, CREATE_OVERWRITE
import REPL: REPL, LineEdit, REPLCompletions

include("show.jl")
include("watch.jl")
include("install.jl")
include("command.jl")
include("repl.jl")

islocal(ctx::Context) = ctx.host in ("127.0.0.1", "localhost")

struct Connection
    ctx::Context
    db::String
    engine::String
end

function connect(db)
    global conn = Connection(Context(load_config()), db, "")
end

function __init__()
    db = get(ENV, "RAI_REPL_DB", "repl")
    connect(db)
    if islocal(conn.ctx)
        julia_repl_hook()
    else
        warn("REPL not initialised on remote connection.")
    end
end

end
