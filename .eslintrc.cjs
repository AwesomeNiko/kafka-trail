module.exports = {
  env: {
    browser: false,
    es2023: true,
    node: true,
    jest: true,
  },
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/eslint-recommended",
    "plugin:@typescript-eslint/recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
    "plugin:@typescript-eslint/strict",
    "plugin:import/recommended",
    "plugin:import/typescript",
    "prettier",
  ],
  globals: {
    Atomics: "readonly",
    SharedArrayBuffer: "readonly",
  },
  plugins: ["import", "unused-imports", "@typescript-eslint"],
  parserOptions: {
    sourceType: "module",
    ecmaVersion: "latest",
    project: './tsconfig.json',
  },
  settings: {
    "import/parsers:": {
      "@typescript-eslint/parser": [".ts", ".tsx"],
    },
    "import/resolver": {
      typescript: true,
      node: true,
    },
    "import/extensions": [".ts"],
    "import/internal-regex": "^src",
  },
  rules: {
    '@typescript-eslint/no-unused-vars': [
      'warn',
      { argsIgnorePattern: '^_' }
    ],
    '@typescript-eslint/consistent-type-imports': 'warn',
    '@typescript-eslint/no-floating-promises': 'error',
    '@typescript-eslint/no-misused-promises': 'error',
    '@typescript-eslint/no-explicit-any': 'error',
    'no-unused-vars': 'off',
    'eqeqeq': 'error',
    'no-var': 'error',
    'prefer-const': 'error',
    indent: ["error", 2, { SwitchCase: 1 }],
    curly: ["error", "multi-line"],
    "object-curly-spacing": ["error", "always"],
    "object-property-newline": ["error", { "allowAllPropertiesOnSameLine": true }],
    "object-curly-newline": [
      "error",
      {
        "ObjectExpression": { "consistent": true, "multiline": true },
        "ObjectPattern": { "consistent": true, "multiline": true },
        "ImportDeclaration": "never",
        "ExportDeclaration": { "multiline": true, "minProperties": 3 }
      }
    ],
    "comma-dangle": ["error", "always-multiline"],
    "block-spacing": "error",
    // import format rules
    "no-restricted-syntax": [
      "error",
      {
        selector:
            "CallExpression[callee.object.name='console'][callee.property.name=/^(log|warn|error|info|trace)$/]",
        message:
            "Don't use console, use pino logger (or another library) instead",
      },
      {
        selector: "TSEnumDeclaration",
        message: "Don't declare enums",
      },
      {
        selector: "CallExpression[callee.property.name='save']",
        message: "Don't use typeorm save, use insert or update instead",
      },
    ],
    "import/no-duplicates": "warn",
    "import/order": [
      "warn",
      {
        alphabetize: {
          order: "asc",
          caseInsensitive: true,
        },
        "newlines-between": "always",
        groups: [
          "builtin",
          "external",
          "internal",
          "parent",
          "index",
          "sibling",
          "unknown",
        ],
        pathGroupsExcludedImportTypes: [],
      },
    ],
    "no-multiple-empty-lines": [
      "error",
      {
        max: 1,
        maxEOF: 1,
        maxBOF: 0,
      },
    ],
    "padding-line-between-statements": [
      "warn",
      {
        blankLine: "always",
        prev: "*",
        next: "return",
      },
      {
        blankLine: "always",
        prev: "*",
        next: "block-like",
      },
      {
        blankLine: "always",
        prev: "block-like",
        next: "*",
      },
    ],
  },
  parser: "",
  root: false,
  noInlineConfig: false,
};
