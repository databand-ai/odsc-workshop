from os import listdir
from os.path import isfile, join
from pathlib import Path

import yaml

import plotly.graph_objects as go

from plotly.subplots import make_subplots


# build dataframe with reporting info from provided path.
def retrieve_report(data):
    name = data["task_def"]["name"]
    execution_date = data["run"]["execution_date"]
    param_inputs = get_params_per_kind(data, "task_input")
    param_outputs = get_params_per_kind(data, "task_output")
    log_location = data["task_run"]["log_local"]
    return name, execution_date, param_inputs, param_outputs, log_location


def get_params_per_kind(data, kind="task_input"):
    params_def = [
        e
        for e in data["task_def"]["task_param_definitions"]
        if e["kind"] == kind and e["group"] == "user"
    ]
    names = list(map(lambda x: x["name"], params_def))
    param_values = [
        (x["parameter_name"], x["value"])
        for x in data["task_run"]["task_run_params"]
        if x["parameter_name"] in names
    ]
    return param_values


def _read_metric_file(path):
    with open(path, "r") as data:
        return " ".join(data.readlines())


def extract_metrics(root):
    return {
        f: _read_metric_file(join(root, f))
        for f in listdir(root)
        if isfile(join(root, f))
    }


def generate_run_report(id, data_path="./data/dev"):
    tasks = []
    metrics = []
    for path in Path(data_path).rglob("meta.yaml"):
        job_name = ""
        name = ""
        with path.open() as stream:
            data = yaml.safe_load(stream)
            if (
                data["run"]["run_uid"] != id
                or data["task_def"]["family"] == "_DbndDriverTask"
            ):
                continue
            tasks.append(retrieve_report(data))
            job_name = data["run"]["job_name"]
            name = data["run"]["name"]

        metrics_path = path.parent.joinpath("metrics", "user")
        if not metrics_path.exists():
            continue
        metrics.append(extract_metrics(metrics_path))

    return tasks, metrics, job_name, name


def generate_task_plotly_figure(task):
    fig = make_subplots(
        rows=2,
        cols=2,
        column_widths=[0.5, 0.5],
        #     row_heights=[0.2, 0.5, 0.3],
        specs=[
            [{"type": "table", "colspan": 2}, None],
            [{"type": "table"}, {"type": "table"}],
        ],
    )

    fig.add_trace(
        go.Table(
            cells=dict(
                values=[
                    ["Log: %s" % task[4], "Execution Date: %s" % task[1]]
                ],  # 1st column
                line_color="darkslategray",
                fill_color="lightcyan",
                align="left",
            )
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Table(
            header=dict(
                values=["Input Param Name", "Value"],
                line_color="darkslategray",
                fill_color="lightskyblue",
                align="left",
            ),
            cells=dict(
                values=[
                    list(map(lambda x: x[0], task[2])),  # 1st column
                    list(map(lambda x: x[1], task[2])),
                ],  # 2nd column
                line_color="darkslategray",
                fill_color="lightcyan",
                align="left",
            ),
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Table(
            header=dict(
                values=["Output Param Name", "Value"],
                line_color="darkslategray",
                fill_color="lightskyblue",
                align="left",
            ),
            cells=dict(
                values=[
                    list(map(lambda x: x[0], task[3])),  # 1st column
                    list(map(lambda x: x[1], task[3])),
                ],  # 2nd column
                line_color="darkslategray",
                fill_color="lightcyan",
                align="left",
            ),
        ),
        row=2,
        col=2,
    )

    fig.update_layout(title_text=task[0], title_x=0.5)
    return fig


def generate_metrics_plotly(metrics, job_name):
    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=["Metric Name", "Value"],
                    line_color="darkslategray",
                    fill_color="lightskyblue",
                    align="left",
                ),
                cells=dict(
                    values=[list(metrics[0].keys()), list(metrics[0].values())],
                    line_color="darkslategray",
                    fill_color="lightcyan",
                    align="left",
                ),
            )
        ]
    )

    fig.update_layout(title_text="%s <br> <br> Run Metrics" % job_name, title_x=0.5)

    return fig


def generate_run_figures(id, data_path="./data/dev"):
    tasks, metrics, job_name, name = generate_run_report(id, data_path)
    if not tasks:
        raise Exception("GUID not found")
    return [generate_metrics_plotly(metrics, job_name)] + [
        generate_task_plotly_figure(t) for t in tasks
    ]


if __name__ == "__main__":
    # build_report(file_path)
    generate_run_figures("6d2080ca-6f9e-11ea-b7b1-dca904774fbf")
