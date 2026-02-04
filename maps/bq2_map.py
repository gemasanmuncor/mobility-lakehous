import folium
import numpy as np
from shapely import wkt
from shapely.geometry import Point

def _generate_points(polygon_wkt, n):
    poly = wkt.loads(polygon_wkt)
    minx, miny, maxx, maxy = poly.bounds
    pts = []
    while len(pts) < n:
        p = Point(
            np.random.uniform(minx, maxx),
            np.random.uniform(miny, maxy)
        )
        if poly.contains(p):
            pts.append(p)
    return pts

def build_bq2_map(df, polygon_wkt, output_path):
    m = folium.Map(location=[38.7, -0.7], zoom_start=7)

    points = _generate_points(polygon_wkt, len(df))
    max_gap = df["gap_score"].abs().max()

    for (_, row), p in zip(df.iterrows(), points):
        color = "red" if row.gap_score > 0 else "green"
        radius = 6 + 18 * (abs(row.gap_score) / max_gap)

        folium.CircleMarker(
            location=[p.y, p.x],
            radius=radius,
            color=color,
            fill=True,
            fill_opacity=0.6,
            popup=f"""
            <b>Infrastructure gap</b><br>
            Month: {row.mes}<br>
            Type: {row.tipo_movilidad}<br>
            Gap score: {row.gap_score:.2f}
            """
        ).add_to(m)

    m.save(output_path)
