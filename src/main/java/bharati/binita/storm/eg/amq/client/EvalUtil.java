package bharati.binita.storm.eg.amq.client;

import clojure.lang.RT;
import clojure.lang.Var;
import clojure.lang.Compiler;

import java.io.IOException;
import java.io.StringReader;

/**
 * 
 * @author binita.bharati@gmail.com
 * Executes Clojure code.
 *
 */

public class EvalUtil {
	
	private static Var findVar(String cljFileName, String ns, String var) throws Exception {
		RT.loadResourceScript(cljFileName);
	    // Get a reference to the foo function.
	    return RT.var(ns, var);    
	}

	private static Object invokeVar(Var var, Object arg0)
	{
		return var.invoke(arg0);
	}
	
	private static Object invokeVar(Var var, Object arg0, Object arg1)
	{
		return var.invoke(arg0, arg1);
	}
	
	public static Object invokeVar(String cljFileName, String ns, String var, Object arg0) throws Exception
	{
		RT.loadResourceScript(cljFileName);
	    // Get a reference to the foo function.
	    Var var1 =  RT.var(ns, var);   
	    return var1.invoke(arg0);
	}
	
	public static Object invokeVar(String cljFileName, String ns, String var, Object arg0, Object arg1) throws Exception
	{
		RT.loadResourceScript(cljFileName);
	    // Get a reference to the foo function.
	    Var var1 =  RT.var(ns, var);   
	    return var1.invoke(arg0, arg1);
	}
	
	public static Object invokeVar(String cljFileName, String ns, String var, Object arg0, Object arg1, Object arg2) throws Exception
	{
		RT.loadResourceScript(cljFileName);
	    // Get a reference to the foo function.
	    Var var1 =  RT.var(ns, var);   
	    return var1.invoke(arg0, arg1, arg2);
	}
	
	
}