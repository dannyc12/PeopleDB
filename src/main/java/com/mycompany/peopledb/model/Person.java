package com.mycompany.peopledb.model;

import com.mycompany.peopledb.annotation.Id;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Person {
    @Id
    private int id;

    private String firstName;
    private String lastName;
    private ZonedDateTime dob;
    private BigDecimal salary = new BigDecimal("0");

    // no args constructor
    public Person(){};

    // constructor without id, because I want the DB to create the id when we add a person
    public Person(String firstName, String lastName, ZonedDateTime dob) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.dob = dob;
    }

    public Person(int id, String firstName, String lastName, ZonedDateTime dob) {
        this(firstName, lastName, dob);
        this.id = id;
    }

    public Person(int id, String firstName, String lastName, ZonedDateTime dob, BigDecimal salary) {
        this(id, firstName, lastName, dob);
        this.salary = salary;
    }

    public ZonedDateTime getDob() {
        return dob;
    }

    public void setDob(ZonedDateTime dob) {
        this.dob = dob;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    public String toString() {
        return String.format("First name: %s, Last name: %s, DOB: %s, ID: %d", firstName, lastName, dob, id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        // use withZoneSameInstant(ZoneId.of("+0") to compare the dobs as if they were using the same time zone
        return id == person.id && firstName.equals(person.firstName) && lastName.equals(person.lastName) &&
                dob.withZoneSameInstant(ZoneId.of("+0")).equals(person.dob.withZoneSameInstant(ZoneId.of("+0")));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstName, lastName, dob);
    }
}
