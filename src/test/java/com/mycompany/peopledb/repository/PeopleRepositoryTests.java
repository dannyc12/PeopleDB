package com.mycompany.peopledb.repository;

import com.mycompany.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {
    private final String url = "jdbc:postgresql://localhost:5432/PeopleTest";
    private final String user = "postgres";
    private final String password = "password";
    private Connection conn;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(url, user, password);
            if (conn !=null) {
                System.out.println("Connection established");
            } else {
                System.out.println("Connection failed");
            }
        } catch (SQLException e) {
            System.out.println(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        repo = new PeopleRepository(conn);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    @Test
    public void canSaveRecord() throws SQLException {
        Person person = new Person("john", "smith", ZonedDateTime.of(1980,11,15, 15,
                0, 0,0, ZoneId.of("-6")));
        Person savedPerson = repo.save(person);
        System.out.println(savedPerson.getId());
        // assertThat is an imported method from assertJ dependency
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoRecords() {
        Person john = new Person("john", "smith", ZonedDateTime.of(1980,11,15, 15,
                0, 0,0, ZoneId.of("-6")));
        Person bobby = new Person("bobby", "smith", ZonedDateTime.of(1982,9,13, 15,
                0, 0,0, ZoneId.of("-6")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bobby);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("test", "tester", ZonedDateTime.now()));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetCount() {
        int startCount = repo.getCount();
        repo.save(new Person("Dan", "Clynes", ZonedDateTime.of(1989,8, 17, 16,0,0,0, ZoneId.of("+0"))));
        repo.save(new Person("Chris", "Clynes", ZonedDateTime.of(1990,9, 29, 16,0,0,0, ZoneId.of("+0"))));
        repo.save(new Person("Stef", "Clynes", ZonedDateTime.of(1988,7, 13, 16,0,0,0, ZoneId.of("+0"))));
        int endCount = repo.getCount();
        assertThat(endCount).isEqualTo(startCount + 3);
    }

    @Test
    public void canDelete() {
        Person savedPerson = repo.save(new Person("Rick", "Clynes", ZonedDateTime.of(1959,10, 3, 16,0,0,0, ZoneId.of("+0"))));
        int startCount = repo.getCount();
        repo.delete(savedPerson);
        int endCount = repo.getCount();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canGetAllPeople() {
        int startCount = repo.getCount();
        repo.save(new Person("Dan", "Clynes", ZonedDateTime.of(1989,8, 17, 16,0,0,0, ZoneId.of("+0"))));
        repo.save(new Person("Chris", "Clynes", ZonedDateTime.of(1990,9, 29, 16,0,0,0, ZoneId.of("+0"))));
        repo.save(new Person("Stef", "Clynes", ZonedDateTime.of(1988,7, 13, 16,0,0,0, ZoneId.of("+0"))));
        List<Person> allPeople = repo.findAll();
        int size = allPeople.size();
        assertThat(size).isEqualTo(startCount + 3);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = repo.save(new Person("Dan", "Clynes", ZonedDateTime.of(1989,8, 17, 16,0,0,0, ZoneId.of("+0"))));
        Person p2 = repo.save(new Person("Chris", "Clynes", ZonedDateTime.of(1990,9, 29, 16,0,0,0, ZoneId.of("+0"))));
        Person p3 = repo.save(new Person("Stef", "Clynes", ZonedDateTime.of(1988,7, 13, 16,0,0,0, ZoneId.of("+0"))));
        int startCount = repo.getCount();
        repo.delete(p1, p2, p3);
        int endCount = repo.getCount();
        assertThat(endCount).isEqualTo(startCount - 3);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("Dan", "Smith", ZonedDateTime.of(1989,8, 17, 16,0,0,0, ZoneId.of("+0"))));
        Person p1 = repo.findById(savedPerson.getId()).get();
        savedPerson.setSalary(new BigDecimal("73000.50"));
        repo.update(savedPerson);
        Person p2 = repo.findById(savedPerson.getId()).get();

        assertThat(p2.getSalary()).isNotEqualByComparingTo(p1.getSalary());
    }

//    @Test
//    public void experiment() {
//        Person p1 = new Person(10, null, null, null);
//        Person p2 = new Person(20, null, null, null);
//        Person p3 = new Person(30, null, null, null);
//        Person p4 = new Person(40, null, null, null);
//        Person p5 = new Person(50, null, null, null);
//
//        // DELETE FROM people WHERE ID IN (10,20,30,40,50);
//
//        // add all the people to an array and instantiate at same time
//        Person[] people = Arrays.asList(p1,p2,p3,p4,p5).toArray(new Person[]{});
//        // arrays.stream takes an array and converts it into a stream, then convert ints to string, then join using commas
//        String ids = Arrays.stream(people).map(Person::getId).map(String::valueOf).collect(Collectors.joining(","));
//        System.out.println(ids);
//    }
}