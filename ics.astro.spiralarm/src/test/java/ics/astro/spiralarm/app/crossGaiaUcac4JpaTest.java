package ics.astro.spiralarm.app;

import ics.astro.spiralarm.dm.crossGaiaUcac4Dm;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.List;

public class crossGaiaUcac4JpaTest {

    private EntityManagerFactory entityManagerFactory;

    @BeforeEach
    void setUp() {
        entityManagerFactory = Persistence.createEntityManagerFactory( "org.hibernate.tutorial.jpa" );

    }

    @AfterEach
    void tearDown() {
        if (entityManagerFactory != null) {
            entityManagerFactory.close();
        }
    }

    @Test
    void testBasicUsage() {
        // create a couple of events...
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        entityManager.persist( new crossGaiaUcac4Dm( ) );
        entityManager.persist( new crossGaiaUcac4Dm( ) );
        entityManager.getTransaction().commit();
        entityManager.close();

        // now lets pull events from the database and list them
        entityManager = entityManagerFactory.createEntityManager();
        entityManager.getTransaction().begin();
        List result = entityManager.createQuery( String.format("from %s", crossGaiaUcac4.tableName), crossGaiaUcac4Dm.class ).getResultList();
        for ( crossGaiaUcac4Dm event : (List<crossGaiaUcac4Dm>) result ) {
            System.out.println( "Data (" + event.getSourceId() + ") : " + event.getUcca4Id() );
        }
        entityManager.getTransaction().commit();
        entityManager.close();
    }
}
