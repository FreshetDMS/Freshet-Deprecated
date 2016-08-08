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

package org.pathirage.freshet.api.data;

import java.util.List;

public interface Type {
  enum TypeName {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOL,
    STRING,
    BYTE,
    BYTES,
    STRUCT,
    ARRAY,
    MAP
  }
  boolean isStruct();
  Type getKeyType();
  Type getValueType();
  Type getElementType();
  TypeName getType();
  List<String> getFieldNames();
  int getFieldCount();
  List<Field> getFields();
  Field getField(String fieldName, boolean caseSensitive);
  Value build(Object o);
}
