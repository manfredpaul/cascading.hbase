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

import static cascading.hbase.helper.Utils.throwIfNullOrEmpty;

/**
 * {@link HBaseFactory} is a lingual compliant provider factory. It makes it
 * possible to talk to HBase via lingual. The factory creates
 * {@link HBaseScheme} intances, that can only talk to one column family at a
 * time. This is done to have an easy way to translate the data to the SQL
 * semantics of lingual.
 */
public class HBaseFactory
  {
  /**
   * LOGGER
   */
  private static final Logger LOG = LoggerFactory.getLogger( HBaseFactory.class );

  /** the name of the protocol */
  public static final String PROTOCOL_NAME = "hbase";

  /** the name of the format */
  public static final String FORMAT_NAME = "hbase";

  /** property containing the column family */
  public static final String FORMAT_COLUMN_FAMILY = "family";

  /**
   * Creates a new {@link Tap} for the given protocol, scheme, identifier and
   * properties.
   *
   * @param protocol   has to be "hbase".
   * @param scheme     a scheme instance, which has to be of type
   *                   {@link HBaseScheme}.
   * @param identifier The identifier of the hbase tap, which is typically the
   *                   table name.
   * @param mode       The sinkMode in which the Tap is used.
   * @param properties A properties object. Currently ignored.
   * @return a new {@link HBaseTap}
   */
  @SuppressWarnings("rawtypes")
  public Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties )
    {
    if( !PROTOCOL_NAME.equals( protocol ) )
      throw new IllegalArgumentException( String.format( "invalid protocol '%s', only '%s' is supported. ", protocol, PROTOCOL_NAME ) );
    LOG.debug( "creating HBaseTap with identifier={} in mode={}", identifier, mode );
    return new HBaseTap( identifier, ( (HBaseScheme) scheme ), mode, 0 );
    }

  /**
   * Creates a new {@link HBaseScheme} for the given format, fields and
   * properties. The {@link Fields} instance has to have at least a size of 2.
   * The first field is used for the rowkey in HBase, all other fields are used
   * for qualifiers in the table. The column family to use has to be given via
   * the properties instance.
   *
   * @param format     only 'hbase' is supported
   * @param fields     a fields instance with at least 2 fields.
   * @param properties a {@link Properties} instance containing the column
   *                   family to use. (see {@link HBaseFactory#FORMAT_COLUMN_FAMILY}.
   * @return a new {@link HBaseScheme} instance.
   */
  @SuppressWarnings("rawtypes")
  public Scheme createScheme( String format, Fields fields, Properties properties )
    {
    LOG.debug( "creating HBaseScheme with fields={}", fields );

    if( !FORMAT_NAME.equals( format ) )
      throw new IllegalArgumentException( String.format( "invalid format '%s', only '%s' is supported. ", format, FORMAT_NAME ) );

    // we only support one column family to keep things simple.
    String familyProperty = properties.getProperty( FORMAT_COLUMN_FAMILY );
    throwIfNullOrEmpty( familyProperty, "column family must be set" );

    if( fields.size() < 2 )
      throw new IllegalArgumentException( "Need at least one key field and one qualifier" );

    // the first field is the rowkey
    Fields keyFields = new Fields( fields.get( 0 ), fields.getTypeClass( 0 ) );

    // the rest is treated as qualifiers
    Comparable[] cmps = new Comparable[ fields.size() - 1 ];
    Type[] types = new Type[ fields.size() - 1 ];
    for( int i = 1; i < fields.size(); i++ )
      {
      cmps[ i - 1 ] = fields.get( i );
      types[ i - 1 ] = fields.getType( i );
      }
    return new HBaseScheme( keyFields, familyProperty, new Fields( cmps, types ) );
    }

  }
