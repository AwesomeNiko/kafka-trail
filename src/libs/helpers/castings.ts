export const ifNanUseDefaultNumber = (value: string | number | undefined, defaultNumber: number) => {
  return isNaN(Number(value))
    ? defaultNumber
    : Number(value);
}