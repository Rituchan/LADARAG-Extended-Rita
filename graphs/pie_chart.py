import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

# Set up grayscale colors
grayscale_colors_roles = ['#333333', '#aaaaaa']
grayscale_colors_noroles = ['#333333', '#888888', '#cccccc']

# Data for pie chart 1: "With roles"
labels_roles = ['SUCCESS', 'ERROR']
values_roles = [94.02, 5.98]

# Data for pie chart 2: "Without roles"
labels_noroles = ['SUCCESS', 'ERROR', 'EXCEPTION']
values_noroles = [94.48, 4.97, 0.55]

# Create subplot with 1 row and 2 columns
fig = make_subplots(rows=1, cols=2, specs=[[{'type':'domain'}, {'type':'domain'}]],
                    subplot_titles=['With roles', 'Without roles'])

# Add the pie charts
fig.add_trace(go.Pie(
    labels=labels_roles,
    values=values_roles,
    marker=dict(
        colors=grayscale_colors_roles,
    ),
    textinfo='percent',
    textposition='outside',
    showlegend=True,
    direction='clockwise',
    sort=False,
    textfont=dict(size=14)
), 1, 1)

fig.add_trace(go.Pie(
    labels=labels_noroles,
    values=values_noroles,
    marker=dict(
        colors=grayscale_colors_noroles,
    ),
    textinfo='percent',
    textposition='outside',
    showlegend=True,
    direction='clockwise',
    sort=False,
    textfont=dict(size=14)
), 1, 2)

# Update layout
fig.update_layout(
    width=600,
    height=600,
    margin=dict(t=50, b=80, l=20, r=20),
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=0,
        xanchor='center',
        x=0.5,
        font=dict(size=10)
    ),
)

# Ensure labels are not cut off
fig.update_traces(automargin=True, showlegend=True)

# Save to PDF
pio.write_image(fig, 'piecharts_grayscale.pdf', format='pdf', width=600, height=600)
