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

import org.apache.avro.Schema;
import org.pathirage.freshet.api.data.Field;
import org.pathirage.freshet.api.data.Type;
import org.pathirage.freshet.api.data.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroType implements Type {

  protected final Schema avroSchema;
  protected final TypeName typeName;

  private final static Map<Schema.Type, AvroType> primitiveTypes =
      new HashMap<>();

  static {
    primitiveTypes.put(Schema.Type.INT,
        new AvroType(Schema.create(Schema.Type.INT)){
          @Override
          public Value build(Object o) {
            return AvroValue.getInt(this, o);
          }
        });
    primitiveTypes.put(Schema.Type.LONG,
        new AvroType(Schema.create(Schema.Type.LONG)){
          @Override
          public Value build(Object o) {
            return AvroValue.getLong(this, o);
          }
        });
    primitiveTypes.put(Schema.Type.FLOAT,
        new AvroType(Schema.create(Schema.Type.FLOAT)){
          @Override
          public Value build(Object o) {
            return AvroValue.getFloat(this, o);
          }
        });
    primitiveTypes.put(Schema.Type.DOUBLE,
        new AvroType(Schema.create(Schema.Type.DOUBLE)){
          @Override
          public Value build(Object o) {
            return AvroValue.getDouble(this, o);
          }
        });
    primitiveTypes.put(Schema.Type.BOOLEAN,
        new AvroType(Schema.create(Schema.Type.BOOLEAN)){
          @Override
          public Value build(Object o) {
            return AvroValue.getBoolean(this, o);
          }
        });
    primitiveTypes.put(Schema.Type.STRING,
        new AvroType(Schema.create(Schema.Type.STRING)){
          @Override
          public Value build(Object o) {
            return AvroValue.getString(this, o);
          }
        });
    primitiveTypes.put(Schema.Type.BYTES,
        new AvroType(Schema.create(Schema.Type.BYTES)){
          @Override
          public Value build(Object o) {
            return AvroValue.getBytes(this, o);
          }
        });
  }

  public static AvroType getType(final Schema avroSchema) {
    TypeName typeName = avroTypeToFreshetType(avroSchema.getType());

    if (!isComplexType(typeName)) {
      return primitiveTypes.get(avroSchema.getType());
    }

    switch (typeName) {
      case STRUCT:
        return new AvroType(avroSchema) {
          @Override
          public boolean isStruct() {
            return true;
          }

          @Override
          public List<String> getFieldNames() {
            List<Schema.Field> fields = avroSchema.getFields();
            List<String> fieldNames = new ArrayList<>();
            for (Schema.Field field : fields) {
              fieldNames.add(field.name());
            }
            return fieldNames;
          }

          @Override
          public int getFieldCount() {
            return avroSchema.getFields().size();
          }

          @Override
          public List<Field> getFields() {
            List<Schema.Field> fields = avroSchema.getFields();
            List<Field> fieldList = new ArrayList<>();
            for (Schema.Field field : fields) {
              fieldList.add(new Field() {
                @Override
                public String getName() {
                  return field.name();
                }

                @Override
                public Type getType() {
                  return AvroType.getType(field.schema());
                }

                @Override
                public int getIndex() {
                  return field.pos();
                }
              });
            }
            return fieldList;
          }

          @Override
          public Field getField(String fieldName, boolean caseSensitive) {
            Schema.Field field = null;

            if (caseSensitive) {
              field = avroSchema.getField(fieldName);
            } else {
              for (String fn : getFieldNames()) {
                if (fn.toLowerCase().equals(fieldName.toLowerCase())) {
                  field = avroSchema.getField(fn);
                }
              }
            }

            if (field == null) {
              throw new IllegalArgumentException("Field with name " + fieldName + " does not exist.");
            }
            final Schema.Field finalField = field;

            return new Field() {
              @Override
              public String getName() {
                return finalField.name();
              }

              @Override
              public Type getType() {
                return AvroType.getType(finalField.schema());
              }

              @Override
              public int getIndex() {
                return finalField.pos();
              }
            };
          }
        };
      case MAP:
        return new AvroType(avroSchema) {
          @Override
          public Type getKeyType() {
            return AvroType.getType(Schema.create(Schema.Type.STRING));
          }

          @Override
          public Type getValueType() {
            return AvroType.getType(avroSchema.getValueType());
          }
        };
      case ARRAY:
        return new AvroType(avroSchema) {
          @Override
          public Type getElementType() {
            return AvroType.getType(avroSchema.getElementType());
          }
        };
      default:
        throw new IllegalArgumentException("Unsupported complex type: " + typeName);
    }
  }

  private static TypeName avroTypeToFreshetType(Schema.Type avroType) {
    switch (avroType) {
      case INT:
        return TypeName.INTEGER;
      case LONG:
        return TypeName.LONG;
      case FLOAT:
        return TypeName.FLOAT;
      case DOUBLE:
        return TypeName.DOUBLE;
      case BOOLEAN:
        return TypeName.BOOL;
      case STRING:
        return TypeName.STRING;
      case BYTES:
        return TypeName.BYTES;
      case RECORD:
        return TypeName.STRUCT;
      case ARRAY:
        return TypeName.ARRAY;
      case MAP:
        return TypeName.MAP;
      default:
        throw new IllegalArgumentException("AvroType: " + avroType + " is not supported.");
    }
  }

  private static boolean isComplexType(TypeName typeName) {
    return typeName == TypeName.ARRAY || typeName == TypeName.MAP || typeName == TypeName.STRUCT;
  }

  private AvroType(Schema avroSchema) {
    this.avroSchema = avroSchema;
    this.typeName = avroTypeToFreshetType(avroSchema.getType());
  }

  @Override
  public boolean isStruct() {
    return false;
  }

  @Override
  public Type getKeyType() {
    throw new UnsupportedOperationException("getValueType is not available for non-key type: " + this.typeName);
  }

  @Override
  public Type getValueType() {
    throw new UnsupportedOperationException("getValueType is not available for non-value type: " + this.typeName);
  }

  @Override
  public Type getElementType() {
    throw new UnsupportedOperationException("getElementType is not available for non-array type: " + this.typeName);
  }

  @Override
  public TypeName getType() {
    return this.typeName;
  }

  @Override
  public List<String> getFieldNames() {
    throw new UnsupportedOperationException("getFieldNames is not available for non-struct type: " + this.typeName);
  }

  @Override
  public int getFieldCount() {
    throw new UnsupportedOperationException("getFieldCount is not available for non-struct type: " + this.typeName);
  }

  @Override
  public List<Field> getFields() {
    throw new UnsupportedOperationException("getFields is not available for non-struct type: " + this.typeName);
  }

  @Override
  public Field getField(String fieldName, boolean caseSensitive) {
    throw new UnsupportedOperationException("getField is not available for non-struct type:" + this.typeName);
  }

  @Override
  public Value build(Object o) {
    switch (avroSchema.getType()) {
      case ARRAY:
        return AvroValue.getArray(this, o);
      case MAP:
        return AvroValue.getMap(this, o);
      case RECORD:
        return AvroValue.getStruct(this, o);
      default:
        throw new UnsupportedOperationException("Building unknown complex type: " + typeName + " is not supported.");
    }
  }
}
