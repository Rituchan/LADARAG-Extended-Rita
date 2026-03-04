import plotly.graph_objects as go
import pandas as pd

# Input data
failures_without_roles = {
    "update restaurant VIP Room": 1,
    "update reservation": 1,
    "update rating": 1,
    "register room": 1,
    "update reservation for customer": 1,
    "update cleaning request": 1,
    "update existing reservation": 1,
    "cancel reservation": 1,
    "update existing rating": 1,
    "update new customer": 1
}

failures_with_roles = {
    "update restaurant VIP Room": 2,
    "update reservation": 1,
    "retrieve rating": 1,
    "update rating": 1,
    "update reservation paid": 1,
    "update cleaning request": 1,
    "update reservation paid": 1,
    "cancel reservation": 1,
    "update existing rating": 1,
    "update customer info": 1
}


# Combine all tasks
all_tasks = sorted(set(failures_with_roles) | set(failures_without_roles))
data = []
for task in all_tasks:
    data.append({
        "Task": task,
        "With Roles": failures_with_roles.get(task, 0),
        "Without Roles": failures_without_roles.get(task, 0)
    })

df = pd.DataFrame(data)
df = df.sort_values(by=["With Roles", "Without Roles"], ascending=False)

# Set y-axis categories
y_tasks = df["Task"].tolist()

# Colors
colors = {
    "With Roles": "#696969",    # dark gray
    "Without Roles": "#D3D3D3"  # light gray
}

# Create traces
fig = go.Figure()

fig.add_trace(go.Bar(
    x=df["With Roles"],
    y=df["Task"],
    name="With Roles",
    orientation='h',
    marker_color=colors["With Roles"]
))

fig.add_trace(go.Bar(
    x=df["Without Roles"],
    y=df["Task"],
    name="Without Roles",
    orientation='h',
    marker_color=colors["Without Roles"]
))

# Add horizontal separator lines
shapes = []
for i in range(len(y_tasks)):
    y_pos = i + 0.5  # halfway between categories
    shapes.append(dict(
        type="line",
        x0=0,
        x1=max(df["With Roles"].max(), df["Without Roles"].max()) + 1,
        y0=y_pos,
        y1=y_pos,
        line=dict(color="lightgray", width=1)
    ))

# Apply layout
fig.update_layout(
    barmode='group',
    xaxis=dict(
        title="Number of failures",
        dtick=1,
        showgrid=False,
        zeroline=False,
        showline=False
    ),
    yaxis=dict(
        title="",
        showgrid=False,
        zeroline=False,
        showline=False,
        categoryorder='array',
        categoryarray=y_tasks
    ),
    plot_bgcolor='white',
    paper_bgcolor='white',
    shapes=shapes,
    height=600,
    margin=dict(l=250, r=40, t=80, b=40),
    showlegend=True
)


# Save to PNG
fig.write_image("most_failing_tasks.pdf", width=500, height=500)
