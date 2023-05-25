package com.mycompany.peopledb.exception;

public class UnableToSaveException extends RuntimeException {
    public UnableToSaveException(String message) {
        super(message);
    }
}
