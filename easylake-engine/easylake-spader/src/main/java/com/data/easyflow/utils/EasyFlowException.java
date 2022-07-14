package com.data.easyflow.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

public class EasyFlowException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ErrorCode errorCode;

    public EasyFlowException(ErrorCode errorCode, String errorMessage) {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    private EasyFlowException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static EasyFlowException asEasyFlowException(ErrorCode errorCode, String message) {
        return new EasyFlowException(errorCode, message);
    }

    public static EasyFlowException asEasyFlowException(ErrorCode errorCode, String message, Throwable cause) {
        if (cause instanceof EasyFlowException) {
            return (EasyFlowException) cause;
        }
        return new EasyFlowException(errorCode, message, cause);
    }

    public static EasyFlowException asEasyFlowException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof EasyFlowException) {
            return (EasyFlowException) cause;
        }
        return new EasyFlowException(errorCode, getMessage(cause), cause);
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
            // return ((Throwable) obj).getMessage();
        } else {
            return obj.toString();
        }
    }
}
