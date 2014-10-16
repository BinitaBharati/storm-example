package bharati.binita.storm.eg4;

import java.util.List;

import org.slf4j.Logger;

public class Util {
	
	public static void logMessage(Logger logger, String currentThreadName,
			String msgFormat, Object... args)
	{
	
		logger.info(currentThreadName +"[INFO]" + String.format(msgFormat, args));
	}

}
