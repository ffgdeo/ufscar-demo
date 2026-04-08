import os
import json
import gradio as gr
import requests
from databricks.sdk import WorkspaceClient

# Auth
w = WorkspaceClient()
host = w.config.host.rstrip("/")

VS_INDEX = "workspace.sistema_academico.exam_chunks_vs_index"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
VOLUME_PATH = "/Volumes/workspace/sistema_academico/staging/exams"


def get_headers():
    try:
        h = w.config._header_factory()
        return {"Authorization": h.get("Authorization", ""), "Content-Type": "application/json"}
    except Exception:
        token = getattr(w.config, 'token', None) or os.environ.get("DATABRICKS_TOKEN", "")
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def search_exams(query, n=3):
    try:
        r = requests.post(
            f"{host}/api/2.0/vector-search/indexes/{VS_INDEX}/query",
            headers=get_headers(),
            json={"columns": ["exam_filename", "chunk"], "query_text": query, "num_results": n},
        )
        if r.status_code != 200:
            return [], f"Vector Search error {r.status_code}: {r.text[:300]}"
        return r.json().get("result", {}).get("data_array", []), None
    except Exception as e:
        return [], str(e)


def query_llm(prompt):
    try:
        r = requests.post(
            f"{host}/serving-endpoints/{LLM_ENDPOINT}/invocations",
            headers=get_headers(),
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
        return f"Erro ({r.status_code}): {r.text[:200]}"
    except Exception as e:
        return f"Erro: {e}"


def download_pdf(filename):
    """Download PDF from volume, return local path or None."""
    try:
        volume_file = f"{VOLUME_PATH}/{filename}.pdf"
        r = requests.get(f"{host}/api/2.0/fs/files{volume_file}", headers=get_headers())
        if r.status_code == 200:
            tmp = f"/tmp/{filename}.pdf"
            with open(tmp, "wb") as f:
                f.write(r.content)
            return tmp
    except Exception:
        pass
    return None


def respond(message, history):
    results, error = search_exams(message)

    if error:
        history.append([message, f"⚠️ Erro na busca: {error}"])
        return history, []

    if not results:
        history.append([message, "Não encontrei provas relevantes. Tente perguntar sobre uma disciplina específica (ex: Cálculo 1, Banco de Dados, Algoritmos)."])
        return history, []

    context_parts, sources = [], []
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
    source_lines = "\n".join([f"- 📄 **{s}**" for s in sources])
    full = f"{response}\n\n---\n**Fontes:**\n{source_lines}"

    # Try to download source PDFs
    files = []
    for s in sources:
        path = download_pdf(s)
        if path:
            files.append(path)

    history.append([message, full])
    return history, files


with gr.Blocks(
    title="Assistente de Provas — UFSCar",
    theme=gr.themes.Soft(),
    css="#chatbot { height: 500px !important; }",
) as app:
    gr.Markdown(
        "# 📚 Assistente de Provas Anteriores\n"
        "**Universidade Federal de São Carlos — UFSCar**\n\n"
        "Pergunte sobre provas anteriores de qualquer disciplina."
    )

    with gr.Row():
        with gr.Column(scale=3):
            chatbot = gr.Chatbot(elem_id="chatbot", show_copy_button=True, render_markdown=True)
            msg = gr.Textbox(placeholder="Pergunte sobre provas anteriores...", label="Sua pergunta", lines=1)
            with gr.Row():
                submit_btn = gr.Button("Enviar", variant="primary", scale=2)
                clear_btn = gr.Button("Limpar", scale=1)
        with gr.Column(scale=1):
            gr.Markdown("### 📎 Fontes (PDFs)")
            files_output = gr.File(label="Provas utilizadas", file_count="multiple", interactive=False)

    gr.Examples(examples=[
        "Quais tópicos caíram na P1 de Cálculo 1 em 2024?",
        "A prova de Banco de Dados tem questões sobre normalização?",
        "Que tipo de exercício aparece na prova de Inteligência Artificial?",
        "Quais assuntos caíram na prova de Probabilidade e Estatística?",
        "A P2 de Algoritmos teve questões sobre grafos?",
    ], inputs=msg)

    msg.submit(respond, [msg, chatbot], [chatbot, files_output]).then(lambda: "", outputs=msg)
    submit_btn.click(respond, [msg, chatbot], [chatbot, files_output]).then(lambda: "", outputs=msg)
    clear_btn.click(lambda: ([], None), outputs=[chatbot, files_output])

app.launch()
