package com.mycompany.peopledb.annotation;

import com.mycompany.peopledb.model.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

// helps application find the annotations while running
@Retention(RetentionPolicy.RUNTIME)
// @Repeatable allows us to call multiple SQL annotations on a method
@Repeatable(MultiSQL.class)
public @interface SQL {
    String value();
    CrudOperation operationType();
}
