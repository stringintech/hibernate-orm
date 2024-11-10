/*
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.id;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.DB2Dialect;
import org.hibernate.dialect.PostgreSQLDialect;

import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.JiraKey;
import org.hibernate.testing.orm.junit.RequiresDialect;
import org.hibernate.testing.orm.junit.RequiresDialects;
import org.hibernate.testing.orm.junit.ServiceRegistry;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.testing.orm.junit.Setting;
import org.junit.jupiter.api.Test;

/**
 * @author Kowsar Atazadeh
 */
@SessionFactory
@RequiresDialects(
		{
				@RequiresDialect(PostgreSQLDialect.class),
				@RequiresDialect(DB2Dialect.class),
		}
)
@ServiceRegistry(
		settings = {
				@Setting(name = AvailableSettings.PREFERRED_POOLED_OPTIMIZER, value = "pooled"),
		}
)
@DomainModel(annotatedClasses = { CteInsertStrategyWithPooledOptimizerTest.Dummy.class })
@JiraKey("HHH-18818")
public class CteInsertStrategyWithPooledOptimizerTest {
	@Test
	void test(SessionFactoryScope scope) {
		// 9 rows inserted with ids 1 to 9
		// two calls to the db for next sequence value generation; first returned 6, second 11
		// ids 10 and 11 are still reserved for the PooledOptimizer
		scope.inTransaction( session -> {
			for ( var i = 1; i <= 9; i++ ) {
				Dummy d = new Dummy( "d" + i );
				session.persist( d );
			}
		} );

		// 9 rows inserted (using CteInsertStrategy) with ids 12 to 20 (before the fix ids would be 16 to 24)
		// two calls to the db for next sequence value generation; first returned 16, second 21
		scope.inTransaction( session -> {
			session.createMutationQuery( "INSERT INTO Dummy (name) SELECT d.name FROM Dummy d" ).
					executeUpdate();
		} );

		// two rows inserted with the reserved ids 10 and 11
		scope.inTransaction( session -> {
			session.persist( new Dummy( "d10" ) );
			session.persist( new Dummy( "d11" ) );
		} );

		// one more row inserted with id 22
		// one call to the db for next sequence value generation which returned 26
		// so new reserved ids for the PooledOptimizer are 22 to 26 where the first one consumed here
		// before the fix this would result in a duplicate id error (since the batch insert inserted rows with ids 16 to 24)
		scope.inTransaction( session -> {
			session.persist( new Dummy( "d22" ) );
		} );
	}

	@Entity(name = "Dummy")
	static class Dummy {
		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "dummy_seq")
		@SequenceGenerator(name = "dummy_seq", sequenceName = "dummy_seq", allocationSize = 5)
		private Long id;

		private String name;

		public Dummy() {
		}

		public Dummy(String name) {
			this.name = name;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
