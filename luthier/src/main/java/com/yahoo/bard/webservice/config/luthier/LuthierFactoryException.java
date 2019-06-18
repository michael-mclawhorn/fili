// Copyright 2019 Oath Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.config.luthier;

/**
 * An exception thrown when Luthier fails to build concepts or resolve dependencies.
 */
public class LuthierFactoryException extends RuntimeException {

    /**
     * Constructor.
     *
     * @param message  Error message text
     */
<<<<<<< HEAD
    LuthierFactoryException(String message) {
=======
    public LuthierFactoryException(String message) {
>>>>>>> 11deba8e7094d18dca7cd7237f20009bf9164fc5
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message Error message text
     * @param cause  throwable triggering this exception.
     */
<<<<<<< HEAD
    LuthierFactoryException(String message, Throwable cause) {
=======
    public LuthierFactoryException(String message, Throwable cause) {
>>>>>>> 11deba8e7094d18dca7cd7237f20009bf9164fc5
        super(message, cause);
    }
}
