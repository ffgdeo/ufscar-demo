import os
import json
import gradio as gr
import requests
from databricks.sdk import WorkspaceClient

# Auth — uses the app's service principal credentials automatically
w = WorkspaceClient()
host = w.config.host
token = w.config.token

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

VS_INDEX = "workspace.sistema_academico.exam_chunks_vs_index"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


def search_exams(query: str, n: int = 3) -> list:
    """Search the Vector Search index for relevant exam chunks."""
    r = requests.post(
        f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query",
        headers=headers,
        json={"columns": ["exam_filename", "chunk"], "query_text": query, "num_results": n},
    )
    if r.status_code != 200:
        return []
    return r.json().get("result", {}).get("data_array", [])


def query_llm(prompt: str) -> str:
    """Call the LLM via the serving endpoint."""
    r = requests.post(
        f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations",
        headers=headers,
        json={
            "messages": [
                {"role": "system", "content": "Você é o assistente acadêmico da UFSCar. Responda sempre em português brasileiro."},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 1024,
        },
    )
    if r.status_code == 200:
        return r.json()["choices"][0]["message"]["content"]
    return f"Erro ao consultar o modelo: {r.status_code}"


def chat(message: str, history: list) -> str:
    """RAG: search exams → build context → ask LLM."""
    # Search for relevant exam content
    results = search_exams(message)

    if not results:
        return "Não encontrei provas relevantes para essa pergunta. Tente perguntar sobre uma disciplina específica (ex: Cálculo 2, Banco de Dados, Algoritmos)."

    # Build context from search results
    context_parts = []
    sources = []
    for r in results:
        filename, chunk = r[0], r[1]
        context_parts.append(f"[Prova: {filename}]\n{chunk}")
        if filename not in sources:
            sources.append(filename)

    context = "\n\n---\n\n".join(context_parts)

    prompt = f"""Baseado APENAS no conteúdo das provas anteriores abaixo, responda a pergunta do aluno.
Se a informação não estiver no contexto, diga que não encontrou dados sobre isso.

## Provas encontradas:
{context}

## Pergunta:
{message}

## Resposta:"""

    response = query_llm(prompt)

    # Append sources
    source_text = ", ".join(sources)
    return f"{response}\n\n---\n📄 *Fontes: {source_text}*"


# Gradio UI
with gr.Blocks(
    title="Assistente de Provas — UFSCar",
    theme=gr.themes.Soft(),
) as app:
    gr.Markdown(
        """
        # 📚 Assistente de Provas Anteriores
        ### Universidade Federal de São Carlos — UFSCar

        Pergunte sobre provas anteriores de qualquer disciplina.
        O sistema busca nos PDFs das provas e responde com base no conteúdo real.
        """
    )

    chatbot = gr.ChatInterface(
        fn=chat,
        examples=[
            "Quais tópicos caíram na P1 de Cálculo 1 em 2024?",
            "A prova de Banco de Dados tem questões sobre normalização?",
            "Que tipo de exercício aparece na prova de Inteligência Artificial?",
            "Quais assuntos caíram na prova de Probabilidade e Estatística?",
            "A P2 de Algoritmos teve questões sobre grafos?",
        ],
        retry_btn=None,
        undo_btn=None,
    )

app.launch()
