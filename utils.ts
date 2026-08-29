import { Type, type TSchema } from "typebox";

/**
 * StringEnum creates a Type.Union of Type.Literal values.
 * This is the recommended way to represent string enums for tool parameters,
 * as it works with Google's API unlike plain Type.Union/Type.Literal.
 */
export function StringEnum<T extends string[]>(
  values: readonly [...T]
): TSchema {
  return Type.Union(values.map((v) => Type.Literal(v)));
}
