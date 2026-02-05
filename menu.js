import ab from "./ab.cjs"
import index from "./index.cjs"
import query from "./query.cjs"

const modules = {
  ab,
  index,
  query
}

export function run(moduleName, ...args) {
  const m = modules[moduleName]
  if (!m) throw new Error(`Module not found: ${moduleName}`)

  if (typeof m === "function") return m(...args)

  if (typeof m.run === "function") return m.run(...args)

  return m
}