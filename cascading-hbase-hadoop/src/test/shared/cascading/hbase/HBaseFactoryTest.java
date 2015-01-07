/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.hbase;

import java.lang.reflect.Type;
import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * Tests for {@link HBaseFactory}.
 */
public class HBaseFactoryTest
  {

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapInvalidProtocol()
    {
    HBaseFactory factory = new HBaseFactory();

    HBaseScheme scheme = Mockito.mock( HBaseScheme.class );
    factory.createTap( "foo", scheme, "table", SinkMode.REPLACE, new Properties() );
    }

  @Test
  public void testCreateTap()
    {
    HBaseFactory factory = new HBaseFactory();

    HBaseScheme scheme = Mockito.mock( HBaseScheme.class );
    Tap<?, ?, ?> tap = factory.createTap( HBaseFactory.PROTOCOL_NAME, scheme, "table", SinkMode.REPLACE, new Properties() );
    assertNotNull( tap );
    assertEquals( scheme, tap.getScheme() );
    assertTrue( tap instanceof HBaseTap );
    HBaseTap htap = (HBaseTap) tap;
    assertSame( SinkMode.REPLACE, htap.getSinkMode() );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeWrongFormat()
    {
    HBaseFactory factory = new HBaseFactory();
    factory.createScheme( "foo", new Fields(), new Properties() );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeNoColumnFamily()
    {

    HBaseFactory factory = new HBaseFactory();

    Fields fields = new Fields( new String[]{"rowkey", "foo"}, new Type[]{String.class, String.class} );
    factory.createScheme( HBaseFactory.FORMAT_NAME, fields, new Properties() );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeInsufficientFields()
    {
    HBaseFactory factory = new HBaseFactory();

    Properties props = new Properties();
    props.setProperty( HBaseFactory.FORMAT_COLUMN_FAMILY, "simpsons" );
    factory.createScheme( HBaseFactory.FORMAT_NAME, new Fields(), props );
    }

  @Test
  public void testCreateSchemeFullyWorking()
    {
    HBaseFactory factory = new HBaseFactory();

    String family = "simpsons";

    Fields fields = new Fields( new String[]{"rowkey", "foo"}, new Type[]{String.class, String.class} );
    Properties props = new Properties();
    props.setProperty( HBaseFactory.FORMAT_COLUMN_FAMILY, family );

    @SuppressWarnings("rawtypes")
    Scheme scheme = factory.createScheme( HBaseFactory.FORMAT_NAME, fields, props );

    assertTrue( scheme instanceof HBaseScheme );
    HBaseScheme hscheme = (HBaseScheme) scheme;

    assertArrayEquals( new String[]{family}, hscheme.getFamilyNames() );

    assertEquals( fields, hscheme.getSinkFields() );
    assertEquals( hscheme.keyField, new Fields( "rowkey", String.class ) );
    }

  }
