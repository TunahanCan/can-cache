package com.can.scripting;

import javax.script.*;
import java.util.Map;

public final class ScriptRegistry
{
    private final ScriptEngine js;

    public ScriptRegistry() {
        ScriptEngineManager m = new ScriptEngineManager();
        this.js = m.getEngineByName("javascript"); // JDK'da varsa kullan, yoksa null
    }

    public boolean jsAvailable() { return js != null; }

    public Object runJs(String code, Map<String,Object> bindings) {
        if (js == null) throw new IllegalStateException("No JS engine available");
        Bindings b = js.createBindings();
        b.putAll(bindings);
        try { return js.eval(code, b); }
        catch (ScriptException e) { throw new RuntimeException(e); }
    }
}
