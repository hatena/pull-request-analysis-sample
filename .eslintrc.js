module.exports = {
  plugins: [],
  extends: ["eslint:recommended", "plugin:import/recommended", "prettier"],
  env: { node: true, es2020: true },
  rules: {
    "sort-imports": "off",
    "import/order": ["error", { alphabetize: { order: "asc" } }],
  },
  overrides: [
    {
      files: ["**/*.ts"],
      extends: [
        "plugin:@typescript-eslint/eslint-recommended",
        "plugin:@typescript-eslint/recommended",
        "prettier/@typescript-eslint",
        "plugin:import/typescript",
      ],
      parser: "@typescript-eslint/parser",
      parserOptions: {
        sourceType: "module",
        project: "./tsconfig.json",
      },
      rules: {},
    },
  ],
};
