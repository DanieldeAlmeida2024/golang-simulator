"""Validacao estrutural de documentos, sem garantir funcionamento de integracoes."""
from pathlib import Path
import unittest

RAIZ = Path(__file__).resolve().parents[1]

class TesteDocumentacao(unittest.TestCase):
    def test_documentos(self):
        for item in ("README.md", "AGENTS.md", "docs/REQUISITOS.md", "docs/ARQUITETURA.md", "docs/UML.puml", "docs/PLANO_TESTES.md"):
            with self.subTest(item=item):
                self.assertTrue((RAIZ / item).is_file())

    def test_uml(self):
        uml = (RAIZ / "docs/UML.puml").read_text(encoding="utf-8")
        self.assertTrue("@startuml" in uml and "@enduml" in uml)
