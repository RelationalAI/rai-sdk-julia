function julia_repl_hook()
    if isdefined(Base, :active_repl)
        repl_init(Base.active_repl)
    else
        atreplinit() do repl
            if isinteractive() && repl isa REPL.LineEditREPL
                isdefined(repl, :interface) || (repl.interface = REPL.setup_interface(repl))
                repl_init(repl)
            end
        end
    end
end

function repl_init(repl)
    main_mode = repl.interface.modes[1]
    rel_mode = create_mode(repl, main_mode)
    push!(repl.interface.modes, rel_mode)
    keymap = Dict{Any,Any}(
        '=' => function (s,args...)
            if isempty(s) || position(LineEdit.buffer(s)) == 0
                buf = copy(LineEdit.buffer(s))
                LineEdit.transition(s, rel_mode) do
                    LineEdit.state(s, rel_mode).input_buffer = buf
                end
            else
                LineEdit.edit_insert(s, '=')
            end
        end
    )
    main_mode.keymap_dict = LineEdit.keymap_merge(main_mode.keymap_dict, keymap)
    return
end

function create_mode(repl, main)
    rel_mode = LineEdit.Prompt("query> ";
    prompt_prefix = repl.options.hascolor ? Base.text_colors[:magenta] : "",
    prompt_suffix = "",
    on_enter = return_callback,
    sticky = true)

    rel_mode.repl = repl
    hp = main.hist
    hp.mode_mapping[:rel] = rel_mode
    rel_mode.hist = hp

    search_prompt, skeymap = LineEdit.setup_search_keymap(hp)
    prefix_prompt, prefix_keymap = LineEdit.setup_prefix_keymap(hp, rel_mode)

    rel_mode.on_done = (s, buf, ok) -> begin
        ok || return REPL.transition(s, :abort)
        input = String(take!(buf))
        REPL.reset(repl)
        repl_eval(repl, input)
        REPL.prepare_next(repl)
        REPL.reset_state(s)
        s.current_mode.sticky || REPL.transition(s, main)
    end

    mk = REPL.mode_keymap(main)

    b = Dict{Any,Any}[
        skeymap, mk, prefix_keymap, LineEdit.history_keymap,
        LineEdit.default_keymap, LineEdit.escape_defaults
    ]
    rel_mode.keymap_dict = LineEdit.keymap(b)
    return rel_mode
end

# This function determines whether we should evaluate or just add a new line
# when the user presses `enter`.
# Ideally, we'd invoke the parser to see if it needs more input.
# For now we have a fairly dumb bracket-based heuristic.
# (which could of course break in the presence of quotes)
function return_callback(s)
    input = String(take!(copy(LineEdit.buffer(s))))
    if count(x -> x in ('(', '['), input) > count(x -> x in (')', ']'), input)
        return false
    end
    return true
end

function evalrel(input; readonly = false)
    try
        result = query(conn, input; readonly)
        Base.eval(Main, :(ans = $result))
        replshow(stdout, result)
    catch
        Base.display_error(stdout, Base.catch_stack())
    end
end

function repl_eval(repl, input)
    c = parsecommand(input)
    if c != nothing
        try # TODO: shouldn't error
            command(c)
        catch
            Base.display_error(stdout, Base.catch_stack())
        end
        return
    end
    evalrel(input)
end
