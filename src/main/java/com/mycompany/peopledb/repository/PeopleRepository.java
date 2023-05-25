package com.mycompany.peopledb.repository;

import com.mycompany.peopledb.annotation.SQL;
import com.mycompany.peopledb.model.CrudOperation;
import com.mycompany.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PeopleRepository extends CRUDRepository<Person> {
    // public static final String SAVE_PERSON_SQL = "INSERT INTO people (first_name, last_name, dob) VALUES (?, ?, ?)";
    public static final String FIND_BY_ID_SQL = "SELECT * FROM people WHERE id = ?";
    public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM people";
    public static final String FIND_ALL_SQL = "SELECT * FROM people";
    public static final String DELETE_BY_ID_SQL = "DELETE FROM people WHERE id = ?";
    public static final String DELETE_BY_ID_IN_SQL = "DELETE FROM people WHERE id IN (:ids)";
    public static final String UPDATE_BY_ID_SQL = "UPDATE people SET first_name = ?, last_name = ?, dob = ?, salary = ? WHERE id = ?";

    // dependency injection design pattern via this constructor
    public PeopleRepository(Connection connection) {
        super(connection);
        //this.connection = connection;
    }

    private Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }

    // custom annotation
    @Override
    @SQL(value = "INSERT INTO people (first_name, last_name, dob) VALUES (?, ?, ?)", operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        // convert ZonedDateTime to GMT +0 and then convert to LocalDateTime
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
    }

    @Override
    @SQL(value = "UPDATE people SET first_name = ?, last_name = ?, dob = ?, salary = ? WHERE id = ?", operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }

    @Override
    @SQL(value = "SELECT * FROM people WHERE id = ?", operationType = CrudOperation.FIND_BY_ID)
    // can also just pass in the reference variable
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_BY_ID_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_BY_ID_IN_SQL, operationType = CrudOperation.DELETE_MANY)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        int personId = rs.getInt("id");
        String firstName = rs.getString("first_name");
        String lastName = rs.getString("last_name");
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("dob").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal("salary");
        return new Person(personId, firstName, lastName, dob, salary);
    }
}
