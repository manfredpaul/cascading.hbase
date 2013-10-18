/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.hbase.helper.Utils.*;

public class HBaseFactory
  {
  /**
   * LOGGER
   */
  private static final Logger LOG = LoggerFactory.getLogger( HBaseFactory.class );

  public static final String FORMAT_FAMILIES = "families";

  @SuppressWarnings("rawtypes")
  public Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties )
    {

    String[] parts = identifier.split( ":" );
    String zkQuorum = parts[ 0 ];
    String tableName = parts[ 1 ];

    return new HBaseTap( zkQuorum, tableName, ( (HBaseScheme) scheme ), mode, 0 );
    }

  @SuppressWarnings("rawtypes")
  public Scheme createScheme( String format, Fields fields, Properties properties )
    {
    String familiesProperty = properties.getProperty( FORMAT_FAMILIES );
    throwIfNullOrEmpty( familiesProperty, "column families must be set" );
    String[] familyNames = familiesProperty.split( ":" );
    if( fields.size() < 2 )
      {
      throw new IllegalArgumentException( "Need at least one key fields and one column family" );
      }
    Fields keyFields = new Fields( fields.get( 0 ), fields.getTypeClass( 0 ) );

    Comparable[] cmps = new Comparable[fields.size() - 1];
    Type[] types = new Type[fields.size() - 1];

    for( int i = 1; i < fields.size(); i++ )
      {
      cmps[ i - 1 ] = fields.get( i );
      types[ i - 1 ] = fields.getType( i );
      }

    return new HBaseScheme( keyFields, familiesProperty, new Fields( cmps, types ) );
    }

  }
