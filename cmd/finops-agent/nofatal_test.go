package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// F-16, F-26: a log.Fatal or panic on a startup path exits without running the deferred
// shutdown, so startup errors are returned to main instead. main alone may call os.Exit.
func TestNoFatalOrPanicOnStartupPaths(t *testing.T) {
	for _, dir := range []string{"../../cmd", "../../pkg/core", "../../pkg/cluster", "../../pkg/nodes"} {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				isMain := path == filepath.Join("../../cmd", "finops-agent", "main.go") && fn.Recv == nil && fn.Name.Name == "main"
				ast.Inspect(fn, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					if name := exitingCall(call); name != "" && !(isMain && name == "os.Exit") {
						t.Errorf("%s: %s on a startup path; return an error instead", fset.Position(call.Pos()), name)
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scanning %s: %v", dir, err)
		}
	}
}

// exitingCall names call if it is panic, os.Exit, or a Fatal or Panic function of a logger.
func exitingCall(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.Ident:
		if fun.Name == "panic" {
			return "panic"
		}
	case *ast.SelectorExpr:
		pkg, ok := fun.X.(*ast.Ident)
		if !ok {
			return ""
		}
		name := pkg.Name + "." + fun.Sel.Name
		if name == "os.Exit" || strings.HasPrefix(fun.Sel.Name, "Fatal") || strings.HasPrefix(fun.Sel.Name, "Panic") {
			return name
		}
	}
	return ""
}
