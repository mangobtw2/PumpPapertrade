import pinoPkg from 'pino';
const pino = pinoPkg;
// Correct JSON serialization of BigInt
declare global {
    interface BigInt {
        toJSON(): Number;
    }
}

BigInt.prototype.toJSON = function () { return Number(this) }

// Create the base logger instance
const baseLogger = pino({
  transport: {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'yyyy-mm-dd HH:MM:ss.l',
      ignore: 'pid,hostname',
    }
  },
  level: 'debug'
});

// Also write to file using a separate logger instance
const fileLogger = pino(pino.destination('.log'));

// Helper function to extract filename from full path
function getFileName(filePath: string): string {
  return filePath.split('/').pop() || filePath;
}

// Function to change log level at runtime
function setLogLevel(level: 'trace' | 'info' | 'debug' | 'error' | 'warn'): void {
  baseLogger.level = level;
  fileLogger.level = level;
}

// Create a wrapper that adds filename to log messages
function createLogger(fileName: string) {
  const shortFileName = getFileName(fileName);
  
  const formatArgs = (args: any[]) => args.map(arg => 
    typeof arg === 'object' ? "\n" + JSON.stringify(arg, null, 2) : arg
  );

  return {
    setLogLevel,
    info: (message: string, ...args: any[]) => {
      const msg = `[${shortFileName}] ${message} ${formatArgs(args).join(' ')}`;
      baseLogger.info(msg);
      fileLogger.info(msg);
    },
    error: (message: string, ...args: any[]) => {
      const msg = `[${shortFileName}] ${message} ${formatArgs(args).join(' ')}`;
      baseLogger.error(msg);
      fileLogger.error(msg);
    },
    warn: (message: string, ...args: any[]) => {
      const msg = `[${shortFileName}] ${message} ${formatArgs(args).join(' ')}`;
      baseLogger.warn(msg);
      fileLogger.warn(msg);
    },
    debug: (message: string, ...args: any[]) => {
      const msg = `[${shortFileName}] ${message} ${formatArgs(args).join(' ')}`;
      baseLogger.debug(msg);
      fileLogger.debug(msg);
    },
    trace: (message: string, ...args: any[]) => {
      const msg = `[${shortFileName}] ${message} ${formatArgs(args).join(' ')}`;
      baseLogger.trace(msg);
      fileLogger.trace(msg);
    },
    fatal: (message: string, ...args: any[]) => {
      const msg = `[${shortFileName}] ${message} ${formatArgs(args).join(' ')}`;
      baseLogger.fatal(msg);
      fileLogger.fatal(msg);
    }
  };
}

export { createLogger };
