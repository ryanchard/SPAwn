"""
Python file metadata extractor for SPAwn.

This module provides functionality for extracting metadata from Python source files.
"""

import ast
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from spawn.metadata import MetadataExtractor

logger = logging.getLogger(__name__)


class PythonMetadataExtractor(MetadataExtractor):
    """Extract metadata from Python source files."""

    supported_extensions = [
        ".py",
        ".pyi",
        ".pyx",  # Cython
        ".pyw",
    ]
    supported_mime_types = [
        "text/x-python",
        "text/x-script.python",
    ]

    def __init__(
        self, analyze_complexity: bool = True, extract_docstrings: bool = True
    ):
        """
        Initialize the Python metadata extractor.

        Args:
            analyze_complexity: Whether to analyze code complexity.
            extract_docstrings: Whether to extract docstrings.
        """
        self.analyze_complexity = analyze_complexity
        self.extract_docstrings = extract_docstrings

    def extract(self, file_path: Path) -> Dict[str, Any]:
        """
        Extract metadata from a Python file.

        Args:
            file_path: Path to the file.

        Returns:
            Dictionary of metadata.
        """
        # Get common file metadata
        metadata = self.add_common_metadata(file_path)

        try:
            # Read the file content
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()

            # Basic file statistics
            metadata["line_count"] = content.count("\n") + 1
            metadata["char_count"] = len(content)

            # Parse the Python code
            try:
                tree = ast.parse(content)

                # Extract module-level docstring
                if self.extract_docstrings and ast.get_docstring(tree):
                    metadata["module_docstring"] = ast.get_docstring(tree)

                # Extract imports
                imports = self._extract_imports(tree)
                if imports:
                    metadata["imports"] = imports

                # Extract classes
                classes = self._extract_classes(tree)
                if classes:
                    metadata["classes"] = classes

                # Extract functions
                functions = self._extract_functions(tree)
                if functions:
                    metadata["functions"] = functions

                # Extract variables
                variables = self._extract_variables(tree)
                if variables:
                    metadata["variables"] = variables

                # Analyze code complexity if requested
                if self.analyze_complexity:
                    complexity = self._analyze_complexity(tree, content)
                    if complexity:
                        metadata["complexity"] = complexity

            except SyntaxError as e:
                metadata["error"] = f"Syntax error: {str(e)}"
                logger.debug(f"Syntax error in {file_path}: {e}")

        except Exception as e:
            logger.error(f"Error extracting Python metadata from {file_path}: {e}")
            metadata["error"] = str(e)

        return metadata

    def _extract_imports(self, tree: ast.Module) -> Dict[str, List[str]]:
        """
        Extract import statements from the AST.

        Args:
            tree: AST of the Python file.

        Returns:
            Dictionary of imports.
        """
        imports = {
            "standard_library": [],
            "third_party": [],
            "local": [],
        }

        standard_libs = set(
            [
                "abc",
                "argparse",
                "ast",
                "asyncio",
                "base64",
                "collections",
                "concurrent",
                "contextlib",
                "copy",
                "csv",
                "datetime",
                "decimal",
                "difflib",
                "enum",
                "functools",
                "glob",
                "gzip",
                "hashlib",
                "http",
                "importlib",
                "inspect",
                "io",
                "itertools",
                "json",
                "logging",
                "math",
                "multiprocessing",
                "os",
                "pathlib",
                "pickle",
                "random",
                "re",
                "shutil",
                "signal",
                "socket",
                "sqlite3",
                "statistics",
                "string",
                "subprocess",
                "sys",
                "tempfile",
                "threading",
                "time",
                "traceback",
                "typing",
                "unittest",
                "urllib",
                "uuid",
                "warnings",
                "weakref",
                "xml",
                "zipfile",
            ]
        )

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    module_name = name.name.split(".")[0]
                    if module_name in standard_libs:
                        imports["standard_library"].append(name.name)
                    else:
                        # Simple heuristic: if it starts with the project name or has a relative import, it's local
                        if module_name.startswith((".", "spawn")):
                            imports["local"].append(name.name)
                        else:
                            imports["third_party"].append(name.name)

            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    module_name = node.module.split(".")[0] if node.module else ""
                    imported_names = [name.name for name in node.names]

                    # Handle relative imports
                    if node.level > 0:
                        imports["local"].append(
                            f"{'.' * node.level}{node.module or ''} -> {', '.join(imported_names)}"
                        )
                    elif module_name in standard_libs:
                        imports["standard_library"].append(
                            f"{node.module} -> {', '.join(imported_names)}"
                        )
                    elif module_name.startswith("spawn"):
                        imports["local"].append(
                            f"{node.module} -> {', '.join(imported_names)}"
                        )
                    else:
                        imports["third_party"].append(
                            f"{node.module} -> {', '.join(imported_names)}"
                        )

        # Remove empty categories
        return {k: v for k, v in imports.items() if v}

    def _extract_classes(self, tree: ast.Module) -> List[Dict[str, Any]]:
        """
        Extract class definitions from the AST.

        Args:
            tree: AST of the Python file.

        Returns:
            List of class information.
        """
        classes = []

        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.ClassDef):
                class_info = {
                    "name": node.name,
                    "line": node.lineno,
                    "end_line": (
                        node.end_lineno if hasattr(node, "end_lineno") else None
                    ),
                }

                # Extract base classes
                if node.bases:
                    class_info["bases"] = [
                        self._get_name_from_expr(base) for base in node.bases
                    ]

                # Extract docstring
                if self.extract_docstrings and ast.get_docstring(node):
                    class_info["docstring"] = ast.get_docstring(node)

                # Extract methods
                methods = []
                for child in ast.iter_child_nodes(node):
                    if isinstance(child, ast.FunctionDef):
                        method_info = {
                            "name": child.name,
                            "line": child.lineno,
                            "end_line": (
                                child.end_lineno
                                if hasattr(child, "end_lineno")
                                else None
                            ),
                        }

                        # Check if it's a special method
                        if child.name.startswith("__") and child.name.endswith("__"):
                            method_info["type"] = "special"
                        # Check if it's a private method
                        elif child.name.startswith("_"):
                            method_info["type"] = "private"
                        else:
                            method_info["type"] = "public"

                        # Extract method docstring
                        if self.extract_docstrings and ast.get_docstring(child):
                            method_info["docstring"] = ast.get_docstring(child)

                        # Extract parameters
                        if child.args:
                            params = []
                            for arg in child.args.args:
                                param = {"name": arg.arg}
                                if arg.annotation:
                                    param["annotation"] = self._get_name_from_expr(
                                        arg.annotation
                                    )
                                params.append(param)

                            if params:
                                method_info["parameters"] = params

                        methods.append(method_info)

                if methods:
                    class_info["methods"] = methods

                classes.append(class_info)

        return classes

    def _extract_functions(self, tree: ast.Module) -> List[Dict[str, Any]]:
        """
        Extract function definitions from the AST.

        Args:
            tree: AST of the Python file.

        Returns:
            List of function information.
        """
        functions = []

        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.FunctionDef) and not isinstance(
                node.parent, ast.ClassDef
            ):
                function_info = {
                    "name": node.name,
                    "line": node.lineno,
                    "end_line": (
                        node.end_lineno if hasattr(node, "end_lineno") else None
                    ),
                }

                # Check if it's a private function
                if node.name.startswith("_"):
                    function_info["type"] = "private"
                else:
                    function_info["type"] = "public"

                # Extract function docstring
                if self.extract_docstrings and ast.get_docstring(node):
                    function_info["docstring"] = ast.get_docstring(node)

                # Extract parameters
                if node.args:
                    params = []
                    for arg in node.args.args:
                        param = {"name": arg.arg}
                        if arg.annotation:
                            param["annotation"] = self._get_name_from_expr(
                                arg.annotation
                            )
                        params.append(param)

                    if params:
                        function_info["parameters"] = params

                # Extract return annotation
                if node.returns:
                    function_info["returns"] = self._get_name_from_expr(node.returns)

                functions.append(function_info)

        return functions

    def _extract_variables(self, tree: ast.Module) -> List[Dict[str, Any]]:
        """
        Extract global variables from the AST.

        Args:
            tree: AST of the Python file.

        Returns:
            List of variable information.
        """
        variables = []

        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        var_info = {
                            "name": target.id,
                            "line": node.lineno,
                        }

                        # Try to determine the type of the value
                        if isinstance(node.value, ast.Str):
                            var_info["type"] = "str"
                        elif isinstance(node.value, ast.Num):
                            var_info["type"] = "num"
                        elif isinstance(node.value, ast.List):
                            var_info["type"] = "list"
                        elif isinstance(node.value, ast.Dict):
                            var_info["type"] = "dict"
                        elif isinstance(node.value, ast.Tuple):
                            var_info["type"] = "tuple"
                        elif isinstance(node.value, ast.Set):
                            var_info["type"] = "set"
                        elif (
                            isinstance(node.value, ast.NameConstant)
                            and node.value.value is None
                        ):
                            var_info["type"] = "None"
                        elif isinstance(node.value, ast.NameConstant) and isinstance(
                            node.value.value, bool
                        ):
                            var_info["type"] = "bool"
                        else:
                            var_info["type"] = "unknown"

                        variables.append(var_info)

            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                var_info = {
                    "name": node.target.id,
                    "line": node.lineno,
                }

                # Extract type annotation
                if node.annotation:
                    var_info["annotation"] = self._get_name_from_expr(node.annotation)

                variables.append(var_info)

        return variables

    def _analyze_complexity(self, tree: ast.Module, content: str) -> Dict[str, Any]:
        """
        Analyze code complexity.

        Args:
            tree: AST of the Python file.
            content: Source code content.

        Returns:
            Dictionary of complexity metrics.
        """
        complexity = {}

        # Count statements
        statement_count = 0
        for node in ast.walk(tree):
            if isinstance(
                node,
                (
                    ast.Assign,
                    ast.AugAssign,
                    ast.Return,
                    ast.Raise,
                    ast.Assert,
                    ast.Import,
                    ast.ImportFrom,
                    ast.If,
                    ast.For,
                    ast.While,
                    ast.Try,
                    ast.ExceptHandler,
                    ast.Pass,
                    ast.Break,
                    ast.Continue,
                ),
            ):
                statement_count += 1

        complexity["statement_count"] = statement_count

        # Count control flow statements
        control_flow = {
            "if": len([n for n in ast.walk(tree) if isinstance(n, ast.If)]),
            "for": len([n for n in ast.walk(tree) if isinstance(n, ast.For)]),
            "while": len([n for n in ast.walk(tree) if isinstance(n, ast.While)]),
            "try": len([n for n in ast.walk(tree) if isinstance(n, ast.Try)]),
        }

        complexity["control_flow"] = control_flow

        # Calculate cyclomatic complexity (McCabe)
        # A simple approximation: 1 + number of branches
        branches = (
            control_flow["if"]
            + control_flow["for"]
            + control_flow["while"]
            + control_flow["try"]
        )
        complexity["cyclomatic_complexity"] = 1 + branches

        # Count comments
        comment_lines = 0
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped.startswith("#"):
                comment_lines += 1

        complexity["comment_lines"] = comment_lines

        # Calculate comment ratio
        total_lines = content.count("\n") + 1
        if total_lines > 0:
            complexity["comment_ratio"] = round(comment_lines / total_lines, 2)

        return complexity

    def _get_name_from_expr(self, expr) -> str:
        """
        Get a string representation of an expression.

        Args:
            expr: AST expression node.

        Returns:
            String representation of the expression.
        """
        if isinstance(expr, ast.Name):
            return expr.id
        elif isinstance(expr, ast.Attribute):
            return f"{self._get_name_from_expr(expr.value)}.{expr.attr}"
        elif isinstance(expr, ast.Subscript):
            return f"{self._get_name_from_expr(expr.value)}[...]"
        elif isinstance(expr, ast.Call):
            return f"{self._get_name_from_expr(expr.func)}(...)"
        elif isinstance(expr, ast.Constant):
            return str(expr.value)
        elif hasattr(ast, "Str") and isinstance(
            expr, ast.Str
        ):  # Python 3.7 compatibility
            return expr.s
        elif hasattr(ast, "Num") and isinstance(
            expr, ast.Num
        ):  # Python 3.7 compatibility
            return str(expr.n)
        else:
            return "..."
