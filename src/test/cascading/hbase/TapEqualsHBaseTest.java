/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class TapEqualsHBaseTest extends HBaseTestCase
  {
  transient private static Map<Object, Object> properties = new HashMap<Object, Object>();

  String inputFile = "src/test/data/small.txt";

  public TapEqualsHBaseTest()
    {
    super( 1, false );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    }

  public void testHBaseTapEquality() throws IOException
    {
    String[] familyNames = {"family1"};
    Fields keyFields = new Fields("keyfield1");
    Fields[] valueFields = new Fields[]{new Fields( "field1" ), new Fields( "field2" )};

    Tap hBaseTap1 = new HBaseTap( "table1", new HBaseScheme( keyFields, familyNames, valueFields ), SinkMode.REPLACE );
    Tap hBaseTap2 = new HBaseTap( "table2", new HBaseScheme( keyFields, familyNames, valueFields ), SinkMode.REPLACE );

    assertFalse("tap1 and tap2 refer to the same object", hBaseTap1.equals(hBaseTap2));
    }
  }