import os
import json
import gradio as gr
import requests
from databricks.sdk import WorkspaceClient

# Auth — Databricks App service principal
w = WorkspaceClient()
host = w.config.host.rstrip("/")

VOLUME_PATH = "/Volumes/workspace/sistema_academico/staging/exams"
VS_INDEX = "workspace.sistema_academico.exam_chunks_vs_index"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"


def get_headers():
    """Get fresh auth headers."""
    try:
        h = w.config._header_factory()
        return {"Authorization": h.get("Authorization", ""), "Content-Type": "application/json"}
    except Exception:
        token = getattr(w.config, 'token', None) or os.environ.get("DATABRICKS_TOKEN", "")
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def search_exams(query: str, n: int = 3) -> tuple:
    """Search the Vector Search index. Returns (results, error)."""
    try:
        headers = get_headers()
        r = requests.post(
            f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query",
            headers=headers,
            json={"columns": ["exam_filename", "chunk"], "query_text": query, "num_results": n},
        )
        if r.status_code != 200:
            return [], f"Vector Search error {r.status_code}: {r.text[:300]}"
        return r.json().get("result", {}).get("data_array", []), None
    except Exception as e:
        return [], str(e)


def download_pdf(filename: str) -> str:
    """Download a PDF from the volume to a temp file and return the path."""
    try:
        headers = get_headers()
        volume_file = f"{VOLUME_PATH}/{filename}.pdf"
        r = requests.get(f"{host}/api/2.0/fs/files{volume_file}", headers=headers)
        if r.status_code == 200:
            tmp_path = f"/tmp/{filename}.pdf"
            with open(tmp_path, "wb") as f:
                f.write(r.content)
            return tmp_path
    except Exception:
        pass
    return None


def query_llm(prompt: str) -> str:
    """Call the LLM via the serving endpoint."""
    try:
        headers = get_headers()
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
        return f"Erro ao consultar o modelo ({r.status_code}): {r.text[:200]}"
    except Exception as e:
        return f"Erro: {e}"


def chat(message: str, history: list) -> tuple:
    """RAG: search exams → build context → ask LLM. Returns (response, files)."""
    results, error = search_exams(message)

    if error:
        return f"⚠️ Erro na busca: {error}", []

    if not results:
        return "Não encontrei provas relevantes para essa pergunta. Tente perguntar sobre uma disciplina específica (ex: Cálculo 1, Banco de Dados, Algoritmos).", []

    # Build context and collect source filenames
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

    # Build source links
    source_lines = []
    for s in sources:
        source_lines.append(f"- 📄 {s}")
    source_text = "\n".join(source_lines)

    full_response = f"{response}\n\n---\n**Fontes utilizadas:**\n{source_text}"

    # Download PDFs for the file viewer
    pdf_files = []
    for s in sources:
        path = download_pdf(s)
        if path:
            pdf_files.append(path)

    return full_response, pdf_files


# Custom CSS for bigger chat area
CSS = """
.chatbot-container .chatbot {
    min-height: 500px !important;
    max-height: 700px !important;
}
#chatbot {
    min-height: 500px !important;
    max-height: 700px !important;
}
.message {
    max-height: none !important;
}
"""

# Gradio UI
with gr.Blocks(
    title="Assistente de Provas — UFSCar",
    theme=gr.themes.Soft(),
    css=CSS,
) as app:
    gr.Markdown(
        """
        # 📚 Assistente de Provas Anteriores
        **Universidade Federal de São Carlos — UFSCar**

        Pergunte sobre provas anteriores de qualquer disciplina.
        O sistema busca nos PDFs das provas e responde com base no conteúdo real.
        """
    )

    with gr.Row():
        with gr.Column(scale=3):
            chatbot = gr.Chatbot(
                label="Chat",
                height=500,
                show_copy_button=True,
                render_markdown=True,
            )
            msg = gr.Textbox(
                placeholder="Pergunte sobre provas anteriores...",
                label="Sua pergunta",
                lines=1,
                scale=4,
            )
            with gr.Row():
                submit_btn = gr.Button("Enviar", variant="primary", scale=2)
                clear_btn = gr.Button("Limpar", scale=1)

        with gr.Column(scale=1):
            gr.Markdown("### 📎 PDFs das Fontes")
            pdf_viewer = gr.File(
                label="Provas utilizadas na resposta",
                file_count="multiple",
                interactive=False,
                height=200,
            )

    gr.Markdown("### Exemplos de perguntas")
    examples = gr.Examples(
        examples=[
            "Quais tópicos caíram na P1 de Cálculo 1 em 2024?",
            "A prova de Banco de Dados tem questões sobre normalização?",
            "Que tipo de exercício aparece na prova de Inteligência Artificial?",
            "Quais assuntos caíram na prova de Probabilidade e Estatística?",
            "A P2 de Algoritmos teve questões sobre grafos?",
        ],
        inputs=msg,
    )

    def respond(message, chat_history):
        response, files = chat(message, chat_history)
        chat_history = chat_history + [[message, response]]
        return "", chat_history, files

    msg.submit(respond, [msg, chatbot], [msg, chatbot, pdf_viewer])
    submit_btn.click(respond, [msg, chatbot], [msg, chatbot, pdf_viewer])
    clear_btn.click(lambda: ([], None), outputs=[chatbot, pdf_viewer])

app.launch()
