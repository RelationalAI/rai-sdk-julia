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

function connect(db)
    global conn = LocalConnection(dbname = db, default_open_mode = OPEN_OR_CREATE)
end

function __init__()
    db = Symbol(get(ENV, "RAI_REPL_DB", "repl"))
    # connect(db)
    julia_repl_hook()
end

end
