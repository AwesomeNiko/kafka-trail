export type Unbrand<T> = T extends string ? string : T extends number ? number : T extends boolean ? boolean : T extends Date ? Date : T extends (infer AT)[] ? AT[] : unknown;
export type Branded<T, BTT extends string | symbol> = T & {
    readonly [key in BTT]: BTT;
};
