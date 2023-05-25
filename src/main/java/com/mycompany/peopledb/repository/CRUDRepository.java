package com.mycompany.peopledb.repository;

import com.mycompany.peopledb.annotation.Id;
import com.mycompany.peopledb.annotation.MultiSQL;
import com.mycompany.peopledb.annotation.SQL;
import com.mycompany.peopledb.exception.UnableToSaveException;
import com.mycompany.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class CRUDRepository<T> {

    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        // getDeclaredMethods() returns an array of method objects from within this class
        // breakdown: if this.getClass() (PeopleRepository) has an annotation called SQL, and that annotation
        // has an operationType equal to the operationType passed in, then if that annotation has a value,
        // return that annotation's value.
        // if it's not there, fall back to the getSaveSql() method
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .flatMap(msql -> Arrays.stream(msql.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(a -> a.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    // throwing exception in the signature as a best practice for documentation, even though it is a runtime exception
    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps =  connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                int id = rs.getInt("id");
                setIdByAnnotation(id, entity);
            }
            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + entity);
        }
        System.out.println(entity.toString());
        return entity;
    }

    public Optional<T> findById(int id) {
        T entity = null;
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return entities;
    }

    public int getCount() {
        int count = 0;
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountSql));
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteSql));
            ps.setInt(1, getIdByAnnotation(entity));
            int affectedRecords = ps.executeUpdate();
            System.out.println(affectedRecords);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Integer getIdByAnnotation(T entity) {
       return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    Integer id = null;
                    try {
                        id = (int)f.get(entity);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated field found"));
    }

    private void setIdByAnnotation(int id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set ID field value");
                    }
                });
    }

    // use var args for parameter to expect an array of unknown number of person objects
    public void delete(T...entities) {
        try {
            Statement st = connection.createStatement();
            String ids = Arrays.stream(entities).map(e -> getIdByAnnotation(e)).map(String::valueOf).collect(Collectors.joining(","));
            int affectedRecords = st.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSql).replace(":ids", ids));
            System.out.println(affectedRecords);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        for (T entity : entities) {
            delete(entity);
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateSql));
            mapForUpdate(entity, ps);
            ps.setInt(5, getIdByAnnotation(entity));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    String getUpdateSql() {
        System.out.println("Couldn't find update annotation");
        return "";
    }

    /**
     *
     * @return should return a SQL string like:
     * "DELETE FROM people WHERE id IN (:ids)"
     * Be sure to include the '(:ids)' named parameter and call it 'ids'
     */
    protected String getDeleteInSql() {throw new RuntimeException("SQL not defined");};

    protected String getDeleteSql() {throw new RuntimeException("SQL not defined");};

    protected String getCountSql() {throw new RuntimeException("SQL not defined");};

    protected String getFindByIdSql() {throw new RuntimeException("SQL not defined");};

    protected String getFindAllSql() {throw new RuntimeException("SQL not defined");};

    protected String getSaveSql() {
        System.out.println("Couldn't find save annotation");
        return "";
    }

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    /**
     *
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's id.
     */

    // an entity can be any class that is designed to work with a database
    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;
}
