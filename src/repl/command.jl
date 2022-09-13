using Markdown

const helpmd = md"""
Welcome to the Rel REPL. These are the commands you can use to manage your
session.

    %help

List this help message.

    %db

See all available databases. The currently attached database will be listed in
bold.

    %db foo

Connect to the database `foo`, creating it if necessary.

    %install foo

Install the source code at the path `foo`. `foo` can be a `.rel` file or a
directory containing `.rel` files.

    %install

List all installed Rel code.

    %install -

Clear all installed Rel code.

    %clear

Clear installed Rel code and the EDB, resetting the database.
"""

parsecommand(s) = match(r"%(\w+)\b\s*(.+)?", s)

function printerr(e)
    printstyled("error: ", color = :red, bold = true)
    print(e)
    println()
end

function hidepwd(path)
    replace(path, pwd() => ".")
end

function command(m::RegexMatch)
    type = m.captures[1]
    arg = m.captures[2]
    if type in ("help", "h")
        println()
        display(helpmd)
    elseif type in ("install", "i")
        if arg == nothing
            list_sources()
        elseif arg == "-"
            reset_sources()
        else
            install(arg)
        end
    elseif type == "db"
        if arg == nothing
            # TODO: list of dbs
            println(conn.dbname)
        else
            connect(Symbol(arg))
        end
    elseif type == "update"
        # TODO error handling
        evalrel(String(read(arg)))
    elseif type == "query"
        evalrel(String(read(arg)), readonly = true)
    elseif type == "clear"
        clear_db()
    else
        display(md"Unrecognised command. Use `%help` to see all options.")
    end
end
