AvroSchema 
==========

AvroSchema support classes.

TODO
---

- ARRAY have only type and item attributes (what about metadata?)
- MAP keys are (assumed?) to be strings
- FIXED size must be integer (must be positive? less than MAXINT?)
- primitive type names cannot have a namespace (so throw an error? or ignore?)
- schema may contain multiple definitions of a named schema
  if definitions are equivalent (?)
- Cleanup default namespace and named schemata handling.
    - For one, it appears to be *too* global. According to the spec,
      we should only be referencing schemas that are named within the
      *enclosing* schema, so those in sibling schemas (say, unions or fields)
      shouldn't be referenced, if I understand the spec correctly.
    - Also, if a named schema is defined more than once in the same schema,
      it must have the same definition: so it appears we *do* need to keep
      track of named schemata globally as well. (And does this play well
      with the requirements regarding enclosing schema?
- default values for bytes and fixed fields are JSON strings,
  where unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255
- make sure other default values for other schema are of appropriate type
- Should AvroField really be an Avro\Schema\AvroSchema object? Avro Fields have
  a name attribute, but not a namespace attribute (and the name can't be namespace
  qualified). It also has additional attributes such as doc, which named schemas
  enum and record have (though not fixed schemas, which also have names), and
  fields also have default and order attributes, shared by no other schema type.
