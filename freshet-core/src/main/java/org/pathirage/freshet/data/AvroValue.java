/**
 * Copyright 2016 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pathirage.freshet.data;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.pathirage.freshet.api.data.Type;
import org.pathirage.freshet.api.data.Value;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class AvroValue implements Value {
  protected final Object value;
  protected final AvroType type;

  public AvroValue(Object value, AvroType type) {
    this.value = value;
    this.type = type;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public int intValue() {
    throw new UnsupportedOperationException("Cannot get int value of unknown data type.");
  }

  @Override
  public long longValue() {
    throw new UnsupportedOperationException("Cannot get long value of unknown data type.");
  }

  @Override
  public float floatValue() {
    throw new UnsupportedOperationException("Cannot get float value of unknown data type.");
  }

  @Override
  public double doubleValue() {
    throw new UnsupportedOperationException("Cannot get double value of unknown data type.");
  }

  @Override
  public boolean booleanValue() {
    throw new UnsupportedOperationException("Cannot get boolean value of unknown data type.");
  }

  @Override
  public String stringValue() {
    throw new UnsupportedOperationException("Cannot get string value of unknown data type.");
  }

  @Override
  public byte byteValue() {
    throw new UnsupportedOperationException("Cannot get byte value of unknown data type.");
  }

  @Override
  public byte[] bytesValue() {
    throw new UnsupportedOperationException("Cannot get bytes value of unknown data type.");
  }

  @Override
  public List<Object> arrayValue() {
    throw new UnsupportedOperationException("Cannot get array value of unknown data type.");
  }

  @Override
  public Map<Object, Object> mapValue() {
    throw new UnsupportedOperationException("Cannot get map value of unknown data type.");
  }

  @Override
  public Value getFieldValue(String fieldName) {
    throw new UnsupportedOperationException("Cannot get field value of unknown data type.");
  }

  @Override
  public Value getFieldValue(int fieldIndex) {
    throw new UnsupportedOperationException("Cannot get field value of unknown data type.");
  }

  @Override
  public Value getElement(int index) {
    throw new UnsupportedOperationException("Cannot get element value of unknown data type.");
  }

  public static AvroValue getInt(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.INTEGER || !(value instanceof Integer)) {
      throw new IllegalArgumentException("Value or type mismatch. Type: " + type.getType() + " value type: " +
          value.getClass().getName());
    }

    return new AvroValue(value, type) {
      @Override
      public int intValue() {
        return ((Integer)value).intValue();
      }
    };
  }

  public static AvroValue getLong(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.LONG || !(value instanceof Long)) {
      throw new IllegalArgumentException("Value or type mismatch. Type: " + type.getType() + " value type: " +
          value.getClass().getName());
    }

    return new AvroValue(value, type) {
      @Override
      public long longValue() {
        return ((Long) value).longValue();
      }
    };
  }

  public static AvroValue getFloat(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.FLOAT || !(value instanceof Float)) {
      throw new IllegalArgumentException("Value or type mismatch. Type: " + type.getType() + " value type: " +
          value.getClass().getName());
    }

    return new AvroValue(value, type) {
      @Override
      public float floatValue() {
        return ((Float)value).floatValue();
      }
    };
  }

  public static AvroValue getDouble(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.DOUBLE || !(value instanceof Double)) {
      throw new IllegalArgumentException("Value or type mismatch. Type: " + type.getType() + " value type: " +
          value.getClass().getName());
    }

    return new AvroValue(value, type) {
      @Override
      public double doubleValue() {
        return ((Double)value).doubleValue();
      }
    };
  }

  public static AvroValue getBoolean(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.BOOL || !(value instanceof Boolean)) {
      throw new IllegalArgumentException("Value or type mismatch. Type: " + type.getType() + " value type: " +
          value.getClass().getName());
    }

    return new AvroValue(value, type) {
      @Override
      public boolean booleanValue() {
        return ((Boolean)value).booleanValue();
      }
    };
  }

  public static AvroValue getString(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.STRING || !(value instanceof String)) {
      throw new IllegalArgumentException("Value or type mismatch. Type: " + type.getType() + " value type: " +
          value.getClass().getName());
    }

    return new AvroValue(value, type) {
      @Override
      public String stringValue() {
        return (String)value;
      }
    };
  }

  public static AvroValue getBytes(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.BYTES || !(value instanceof ByteBuffer)) {
      throw new IllegalArgumentException("Value or type mismatch. Type: " + type.getType() + " value type: " +
          value.getClass().getName());
    }

    return new AvroValue(value, type) {
      @Override
      public byte[] bytesValue() {
        return ((ByteBuffer)value).array();
      }
    };
  }

  public static AvroValue getArray(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.ARRAY) {
      throw new IllegalArgumentException("Can't create an array value with non-array schema: " + type.getType());
    }

    return new AvroValue(value, type) {
      private final GenericArray<Object> array = (GenericArray<Object>)value;

      @Override
      public List<Object> arrayValue() {
        return array;
      }

      @Override
      public Value getElement(int index) {
        return type.getElementType().build(array.get(index));
      }
    };
  }

  public static AvroValue getMap(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.MAP) {
      throw new IllegalArgumentException("Can't create a map value with non-map schema: " + type.getType());
    }

    return new AvroValue(value, type) {
      private final Map<Object, Object> map = (Map<Object, Object>)value;

      @Override
      public Map<Object, Object> mapValue() {
        return map;
      }

      @Override
      public Value getFieldValue(String fieldName) {
        // Avro map's key type defaults to String
        return type.getValueType().build(map.get(fieldName));
      }
    };
  }

  public static AvroValue getStruct(AvroType type, Object value) {
    if(type.getType() != Type.TypeName.STRUCT) {
      throw new IllegalArgumentException("Can't create a struct value with non-struct schema: " + type.getType());
    }
    return new AvroValue(value, type){
      private final GenericRecord record = (GenericRecord)value;

      @Override
      public Value getFieldValue(String fieldName) {
        return type.getField(fieldName, true).getType().build(record.get(fieldName));
      }

      @Override
      public Value getFieldValue(int fieldIndex) {
        return type.getFields().get(fieldIndex).getType().build(record.get(fieldIndex));
      }
    };
  }
}
