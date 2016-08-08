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
import java.util.Map;

public interface Value {
  Type getType();

  Object value();

  int intValue();

  long longValue();

  float floatValue();

  double doubleValue();

  boolean booleanValue();

  String stringValue();

  byte byteValue();

  byte[] bytesValue();

  List<Object> arrayValue();

  Map<Object, Object> mapValue();

  Value getFieldValue(String fieldName);

  Value getFieldValue(int fieldIndex);

  Value getElement(int index);
}
