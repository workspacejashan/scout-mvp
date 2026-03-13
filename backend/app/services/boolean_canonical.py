from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal
from typing import Optional


TokenType = Literal["LPAREN", "RPAREN", "AND", "OR", "QUOTED", "WORD"]


@dataclass(frozen=True)
class Token:
    t: TokenType
    v: str


def _tokenize(s: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(s)

    while i < n:
        ch = s[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "(":
            tokens.append(Token("LPAREN", ch))
            i += 1
            continue
        if ch == ")":
            tokens.append(Token("RPAREN", ch))
            i += 1
            continue
        if ch == '"':
            i += 1
            buf = []
            while i < n:
                if s[i] == "\\" and i + 1 < n:
                    buf.append(s[i + 1])
                    i += 2
                    continue
                if s[i] == '"':
                    i += 1
                    break
                buf.append(s[i])
                i += 1
            tokens.append(Token("QUOTED", "".join(buf)))
            continue

        m = re.match(r"[A-Za-z0-9_]+", s[i:])
        if m:
            word = m.group(0)
            upper = word.upper()
            if upper == "AND":
                tokens.append(Token("AND", "AND"))
            elif upper == "OR":
                tokens.append(Token("OR", "OR"))
            else:
                tokens.append(Token("WORD", word))
            i += len(word)
            continue

        # fallback: single char as WORD
        tokens.append(Token("WORD", ch))
        i += 1

    return tokens


NodeKind = Literal["TERM", "AND", "OR"]


@dataclass
class Node:
    kind: NodeKind
    value: Optional[str] = None
    children: Optional[list["Node"]] = None


class _Parser:
    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.i = 0

    def _peek(self) -> Optional[Token]:
        if self.i >= len(self.tokens):
            return None
        return self.tokens[self.i]

    def _eat(self, t: Optional[TokenType] = None) -> Token:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of input")
        if t is not None and tok.t != t:
            raise ValueError(f"Expected {t}, got {tok.t}")
        self.i += 1
        return tok

    def parse(self) -> Node:
        node = self._parse_or()
        if self._peek() is not None:
            raise ValueError("Unexpected trailing tokens")
        return node

    def _parse_or(self) -> Node:
        left = self._parse_and()
        parts = [left]
        while True:
            tok = self._peek()
            if tok and tok.t == "OR":
                self._eat("OR")
                parts.append(self._parse_and())
            else:
                break
        if len(parts) == 1:
            return left
        return Node(kind="OR", children=parts)

    def _parse_and(self) -> Node:
        left = self._parse_term()
        parts = [left]
        while True:
            tok = self._peek()
            if tok and tok.t == "AND":
                self._eat("AND")
                parts.append(self._parse_term())
            else:
                break
        if len(parts) == 1:
            return left
        return Node(kind="AND", children=parts)

    def _parse_term(self) -> Node:
        tok = self._peek()
        if tok is None:
            raise ValueError("Expected term, got end of input")
        if tok.t == "LPAREN":
            self._eat("LPAREN")
            inner = self._parse_or()
            self._eat("RPAREN")
            return inner
        if tok.t == "QUOTED":
            self._eat("QUOTED")
            return Node(kind="TERM", value=tok.v)
        if tok.t == "WORD":
            self._eat("WORD")
            return Node(kind="TERM", value=tok.v)
        raise ValueError(f"Unexpected token {tok.t}")


def _canon_term(s: str) -> str:
    # Case-insensitive, but do NOT normalize punctuation.
    s2 = re.sub(r"\s+", " ", s.strip())
    return s2.lower()


def _canon(node: Node) -> str:
    obj = _canon_obj(node)
    return _canon_str(obj)

def _canon_obj(node: Node):
    if node.kind == "TERM":
        return ("T", _canon_term(node.value or ""))

    op = "AND" if node.kind == "AND" else "OR"
    parts: set[tuple] = set()
    for child in node.children or []:
        cobj = _canon_obj(child)
        if isinstance(cobj, tuple) and len(cobj) == 2 and cobj[0] == op and isinstance(cobj[1], tuple):
            # flatten
            parts.update(cobj[1])
        else:
            parts.add(cobj)

    # stable ordering
    parts_sorted = tuple(sorted(parts, key=lambda x: repr(x)))
    return (op, parts_sorted)


def _canon_str(obj) -> str:
    if obj[0] == "T":
        return f'T("{obj[1]}")'
    op = obj[0]
    children = obj[1]
    return f"{op}({','.join(_canon_str(c) for c in children)})"


def canonicalize_boolean(boolean_text: str) -> tuple[str, str]:
    """
    Returns (canonical_string, signature_hex).
    Canonicalization is commutative for AND/OR and case-insensitive for terms.
    """
    tokens = _tokenize(boolean_text)
    if not tokens:
        raise ValueError("Empty boolean")
    ast = _Parser(tokens).parse()
    canon = _canon(ast)
    sig = hashlib.sha256(canon.encode("utf-8")).hexdigest()
    return canon, sig


def parse_boolean(boolean_text: str) -> Node:
    """Parse a boolean string into an AST (AND/OR with parentheses)."""
    tokens = _tokenize(boolean_text)
    if not tokens:
        raise ValueError("Empty boolean")
    return _Parser(tokens).parse()


def boolean_matches_text(boolean_text: str, haystack: str) -> bool:
    """
    Evaluate a boolean string against a text blob.
    Rules:
    - Terms are matched case-insensitively
    - Quoted phrases are matched as substrings (whitespace-collapsed)
    - Single WORD terms are matched on word boundaries when possible
    """
    ast = parse_boolean(boolean_text)
    text = _norm_text(haystack or "")
    return _eval(ast, text)


def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def _term_matches(term: str, text: str) -> bool:
    t = _norm_text(term)
    if not t:
        return False
    # If the term contains spaces, treat as a phrase substring.
    if " " in t:
        return t in text
    # Word-boundary match for token-like terms to avoid RN matching "brian".
    # Fallback to substring when boundaries can't apply.
    try:
        pat = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(t)}(?![A-Za-z0-9_])", flags=re.IGNORECASE)
        return pat.search(text) is not None
    except Exception:
        return t in text


def _eval(node: Node, text: str) -> bool:
    if node.kind == "TERM":
        return _term_matches(node.value or "", text)
    if node.kind == "AND":
        return all(_eval(c, text) for c in (node.children or []))
    if node.kind == "OR":
        return any(_eval(c, text) for c in (node.children or []))
    return False

