"""
3D Design Studio — Streamlit + Three.js + Cloudflare R2 (Parquet)
Architecture : Plan Editor (transforms) + Object Designer (points/segments)
Unités       : centimètres, précision millimètre (0.1 cm)
"""

import io
import json
import math
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import streamlit as st
from streamlit.components.v1 import html as st_html

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="3D Design Studio",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'JetBrains Mono', monospace;
    }

    /* Dark theme tokens */
    :root {
        --bg0: #0a0c10;
        --bg1: #0f1117;
        --bg2: #161b22;
        --bg3: #1c2333;
        --border: #21262d;
        --accent: #58a6ff;
        --accent2: #3fb950;
        --accent3: #f78166;
        --text0: #e6edf3;
        --text1: #8b949e;
        --text2: #484f58;
    }

    .stApp { background: var(--bg0); }

    section[data-testid="stSidebar"] {
        background: var(--bg1) !important;
        border-right: 1px solid var(--border);
    }

    section[data-testid="stSidebar"] > div { padding-top: 0.5rem; }

    .main .block-container {
        padding: 0.75rem 1rem 1rem 1rem;
        max-width: 100%;
    }

    /* Studio header */
    .studio-header {
        display: flex; align-items: center; gap: 10px;
        padding: 12px 0 8px 0;
        border-bottom: 1px solid var(--border);
        margin-bottom: 12px;
    }
    .studio-title {
        font-family: 'Syne', sans-serif;
        font-size: 18px; font-weight: 800;
        color: var(--accent);
        letter-spacing: -0.5px;
    }
    .studio-sub {
        font-size: 9px; color: var(--text2);
        letter-spacing: 2px; text-transform: uppercase;
    }

    /* Mode badge */
    .badge {
        display: inline-block;
        padding: 2px 8px; border-radius: 4px;
        font-size: 10px; font-weight: 600;
        letter-spacing: 1px; text-transform: uppercase;
    }
    .badge-plan   { background: #1a2744; color: #58a6ff; border: 1px solid #1f3a72; }
    .badge-object { background: #2a1a1a; color: #f78166; border: 1px solid #5a2a2a; }

    /* Section labels */
    .section-label {
        font-size: 9px; letter-spacing: 2px; text-transform: uppercase;
        color: var(--text2); margin: 8px 0 4px 0;
    }

    /* Metric cards */
    .metric-row {
        display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px;
        margin: 8px 0;
    }
    .metric-card {
        background: var(--bg2); border: 1px solid var(--border);
        border-radius: 6px; padding: 8px 10px;
        text-align: center;
    }
    .metric-val { font-size: 18px; font-weight: 700; color: var(--accent); }
    .metric-lbl { font-size: 9px; color: var(--text2); letter-spacing: 1px; text-transform: uppercase; }

    /* Viewer wrapper */
    .viewer-wrap {
        border-radius: 8px; overflow: hidden;
        border: 1px solid var(--border);
        background: #0a0c10;
    }

    /* Object list item */
    .obj-item {
        display: flex; align-items: center; gap: 6px;
        padding: 5px 8px; border-radius: 4px;
        cursor: pointer; border: 1px solid transparent;
        font-size: 12px; color: var(--text0);
        transition: all 0.15s ease;
    }
    .obj-item.active {
        background: var(--bg3);
        border-color: var(--accent);
        color: var(--accent);
    }

    /* Streamlit overrides */
    .stButton > button {
        background: var(--bg2) !important;
        border: 1px solid var(--border) !important;
        color: var(--text0) !important;
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 11px !important;
        border-radius: 5px !important;
        transition: all 0.15s ease !important;
    }
    .stButton > button:hover {
        border-color: var(--accent) !important;
        color: var(--accent) !important;
    }

    .stTabs [data-baseweb="tab"] {
        font-family: 'JetBrains Mono', monospace;
        font-size: 11px;
    }

    div[data-testid="stNumberInput"] input {
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
        background: var(--bg2) !important;
        border-color: var(--border) !important;
        color: var(--text0) !important;
    }

    div[data-testid="stDataFrame"] { font-size: 11px; }

    .stAlert { font-size: 11px; }

    /* Info box */
    .info-box {
        background: var(--bg2); border: 1px solid var(--border);
        border-radius: 6px; padding: 10px 12px;
        font-size: 11px; color: var(--text1);
        margin: 8px 0;
    }
    .info-box code {
        background: var(--bg3); padding: 1px 4px;
        border-radius: 3px; color: var(--accent2);
        font-size: 10px;
    }

    /* Success inline */
    .tag-success {
        display: inline-block; background: #1a3a1a;
        color: var(--accent2); border: 1px solid #2a5a2a;
        padding: 2px 6px; border-radius: 3px; font-size: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# R2 / PARQUET LAYER
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{st.secrets['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=st.secrets["R2_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["R2_SECRET_KEY"],
        region_name="auto",
    )


def load_parquet(key: str, cols: list[str]) -> pd.DataFrame:
    try:
        obj = get_r2().get_object(Bucket=st.secrets["R2_BUCKET"], Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        return pd.DataFrame(columns=cols)


def save_parquet(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="zstd")
    buf.seek(0)
    get_r2().put_object(
        Bucket=st.secrets["R2_BUCKET"], Key=key, Body=buf.getvalue()
    )


# ── Clés & schémas ────────────────────────────────────────────────────────────

PROJ_KEY = "projects.parquet"
OBJ_KEY  = "objects.parquet"
PTS_KEY  = "points.parquet"
SEG_KEY  = "segments.parquet"

PROJ_COLS = ["project_id", "name", "created_at"]
OBJ_COLS  = [
    "object_id", "project_id", "name",
    "pos_x", "pos_y", "pos_z",
    "rot_x", "rot_y", "rot_z", "rot_w",
    "scale_x", "scale_y", "scale_z",
]
PTS_COLS  = ["point_id", "object_id", "x", "y", "z"]
SEG_COLS  = ["segment_id", "object_id", "point_a_id", "point_b_id"]


def next_id(df: pd.DataFrame, col: str) -> int:
    if df.empty or col not in df.columns or df[col].isnull().all():
        return 1
    return int(df[col].max()) + 1


def init_r2_tables() -> None:
    """Crée les fichiers Parquet dans R2 s'ils n'existent pas."""
    for key, cols in [
        (PROJ_KEY, PROJ_COLS),
        (OBJ_KEY,  OBJ_COLS),
        (PTS_KEY,  PTS_COLS),
        (SEG_KEY,  SEG_COLS),
    ]:
        df = load_parquet(key, cols)
        if df.empty:
            try:
                save_parquet(pd.DataFrame(columns=cols), key)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────

def _ss(key: str, default):
    if key not in st.session_state:
        st.session_state[key] = default


def init_session() -> None:
    _ss("mode",              "plan_editor")   # "plan_editor" | "object_designer"
    _ss("project_id",        None)
    _ss("object_id",         None)
    _ss("selected_pts",      [])
    _ss("selected_seg",      None)
    _ss("show_grid",         True)
    _ss("show_axes",         True)
    _ss("snap",              True)
    _ss("snap_dist",         5.0)
    _ss("camera_preset",     "perspective")
    _ss("r2_ready",          False)


# ─────────────────────────────────────────────────────────────────────────────
# THREE.JS VIEWER — HTML COMPONENT
# ─────────────────────────────────────────────────────────────────────────────

def _build_scene_json(
    project_id,
    objects_df: pd.DataFrame,
    points_df:  pd.DataFrame,
    segments_df: pd.DataFrame,
    sel_obj:  int | None,
    sel_pts:  list[int],
) -> dict:
    """Construit le payload JSON envoyé au viewer Three.js."""

    scene = {
        "objects":   [],
        "showGrid":  st.session_state.get("show_grid", True),
        "showAxes":  st.session_state.get("show_axes", True),
        "snap":      st.session_state.get("snap", True),
        "snapDist":  st.session_state.get("snap_dist", 5.0),
        "unitScale": 0.01,   # cm → m pour Three.js
    }

    if project_id is None or objects_df.empty:
        return scene

    proj_objs = objects_df[objects_df["project_id"] == project_id]

    for _, obj in proj_objs.iterrows():
        oid = int(obj["object_id"])
        o_pts  = points_df[points_df["object_id"] == oid]   if not points_df.empty   else pd.DataFrame()
        o_segs = segments_df[segments_df["object_id"] == oid] if not segments_df.empty else pd.DataFrame()

        pts = []
        for _, p in o_pts.iterrows():
            pts.append({
                "id": int(p["point_id"]),
                "x":  float(p["x"]),
                "y":  float(p["y"]),
                "z":  float(p["z"]),
                "sel": int(p["point_id"]) in sel_pts,
            })

        segs = []
        for _, s in o_segs.iterrows():
            segs.append({
                "id": int(s["segment_id"]),
                "a":  int(s["point_a_id"]),
                "b":  int(s["point_b_id"]),
            })

        scene["objects"].append({
            "id":   oid,
            "name": str(obj["name"]),
            "pos":  {"x": float(obj["pos_x"]), "y": float(obj["pos_y"]), "z": float(obj["pos_z"])},
            "rot":  {"x": float(obj["rot_x"]), "y": float(obj["rot_y"]), "z": float(obj["rot_z"]), "w": float(obj["rot_w"])},
            "scl":  {"x": float(obj["scale_x"]), "y": float(obj["scale_y"]), "z": float(obj["scale_z"])},
            "points":   pts,
            "segments": segs,
            "sel": oid == sel_obj,
        })

    return scene


def render_viewer(scene: dict, mode: str, height: int = 560) -> None:
    """Génère et affiche le composant HTML Three.js."""

    scene_json = json.dumps(scene)
    is_plan    = mode == "plan_editor"
    badge_cls  = "badge-plan" if is_plan else "badge-object"
    badge_lbl  = "PLAN EDITOR" if is_plan else "OBJECT DESIGNER"

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  *{{ margin:0; padding:0; box-sizing:border-box; }}
  body{{ background:#0a0c10; overflow:hidden;
        font-family:'JetBrains Mono',monospace; }}

  #wrap{{ width:100%; height:{height}px; position:relative; }}

  /* HUD badges */
  .hud{{
    position:absolute; pointer-events:none;
    font-size:10px; letter-spacing:.5px;
  }}
  #badge{{
    top:10px; left:10px;
    padding:4px 10px; border-radius:4px; font-weight:700;
    letter-spacing:1.5px; text-transform:uppercase;
  }}
  .badge-plan{{ background:#1a2744; color:#58a6ff; border:1px solid #1f3a72; }}
  .badge-object{{ background:#2a1a1a; color:#f78166; border:1px solid #5a2a2a; }}

  #coords{{
    bottom:10px; left:10px;
    color:#3fb950; background:rgba(0,0,0,.6);
    padding:6px 10px; border-radius:4px;
  }}
  #help{{
    top:10px; right:10px;
    color:#484f58; background:rgba(0,0,0,.5);
    padding:8px 12px; border-radius:6px;
    line-height:1.8;
  }}
  #status{{
    bottom:10px; right:10px;
    color:#8b949e; background:rgba(0,0,0,.5);
    padding:5px 10px; border-radius:4px;
  }}

  /* Selection tooltip */
  #tooltip{{
    position:absolute; display:none;
    background:#161b22; border:1px solid #21262d;
    color:#e6edf3; font-size:10px;
    padding:5px 8px; border-radius:4px;
    pointer-events:none; white-space:nowrap;
  }}
</style>
</head>
<body>
<div id="wrap">
  <div id="badge" class="hud {badge_cls}">{badge_lbl}</div>

  <div id="help" class="hud">
    🖱 Rotate: right drag &nbsp;·&nbsp; Pan: mid drag &nbsp;·&nbsp; Zoom: scroll<br>
    ⌨ Arrows: move sel. &nbsp;·&nbsp; Ctrl: ×0.1 &nbsp;·&nbsp; Shift: ×10<br>
    R: rotate mode &nbsp;·&nbsp; S: scale mode &nbsp;·&nbsp; E: segment mode
  </div>

  <div id="coords" class="hud">X: 0.0 · Y: 0.0 · Z: 0.0 cm</div>
  <div id="status" class="hud">Ready</div>
  <div id="tooltip"></div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
// ──────────────────────────────────────────────
// DATA
// ──────────────────────────────────────────────
const SCENE   = {scene_json};
const MODE    = {json.dumps(mode)};
const US      = SCENE.unitScale; // 0.01  cm→m

// ──────────────────────────────────────────────
// RENDERER / CAMERA / SCENE
// ──────────────────────────────────────────────
const wrap = document.getElementById('wrap');
const W = wrap.clientWidth, H = {height};

const renderer = new THREE.WebGLRenderer({{antialias:true, alpha:false}});
renderer.setSize(W, H);
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setClearColor(0x0a0c10);
renderer.shadowMap.enabled = true;
wrap.appendChild(renderer.domElement);

const scene  = new THREE.Scene();
scene.fog    = new THREE.FogExp2(0x0a0c10, 0.008);

const camera = new THREE.PerspectiveCamera(55, W/H, 0.01, 5000);
camera.position.set(8, 6, 12);
camera.lookAt(0,0,0);

// ──────────────────────────────────────────────
// LIGHTS
// ──────────────────────────────────────────────
const ambient = new THREE.AmbientLight(0xffffff, 0.4);
scene.add(ambient);

const dir = new THREE.DirectionalLight(0x8ab4f8, 0.8);
dir.position.set(10, 20, 10);
scene.add(dir);

// ──────────────────────────────────────────────
// ORBIT CONTROLS (custom, lightweight)
// ──────────────────────────────────────────────
let sph    = {{theta: 0.6, phi: 0.9, r: 18}};
let target = new THREE.Vector3();
let isRD   = false, isMD = false;
let lm     = {{x:0, y:0}};

function applyCamera() {{
  const sp = Math.sin(sph.phi), cp = Math.cos(sph.phi);
  const st = Math.sin(sph.theta), ct = Math.cos(sph.theta);
  camera.position.set(
    target.x + sph.r * sp * st,
    target.y + sph.r * cp,
    target.z + sph.r * sp * ct
  );
  camera.lookAt(target);
}}
applyCamera();

const cv = renderer.domElement;

cv.addEventListener('contextmenu', e => e.preventDefault());

cv.addEventListener('mousedown', e => {{
  if (e.button===2){{ isRD=true; }}
  if (e.button===1){{ isMD=true; e.preventDefault(); }}
  lm={{x:e.clientX, y:e.clientY}};
}});
window.addEventListener('mouseup', () => {{ isRD=false; isMD=false; }});

window.addEventListener('mousemove', e => {{
  const dx=e.clientX-lm.x, dy=e.clientY-lm.y;
  lm={{x:e.clientX, y:e.clientY}};

  if (isRD) {{
    sph.theta -= dx*0.005;
    sph.phi    = Math.max(0.05, Math.min(Math.PI-0.05, sph.phi+dy*0.005));
    applyCamera();
  }}
  if (isMD) {{
    const sp = sph.r * 0.0008;
    const right = new THREE.Vector3();
    right.crossVectors(camera.getWorldDirection(new THREE.Vector3()), camera.up).normalize();
    target.addScaledVector(right, -dx*sp);
    target.addScaledVector(camera.up, dy*sp);
    applyCamera();
  }}
  updateCoords(e);
}});

cv.addEventListener('wheel', e => {{
  e.preventDefault();
  sph.r = Math.max(0.3, Math.min(800, sph.r * (1 + e.deltaY*0.001)));
  applyCamera();
}}, {{passive:false}});

cv.addEventListener('dblclick', () => {{
  // Focus sur centroïde des objets sélectionnés
  target.set(0, 0, 0); sph.r=18; applyCamera();
}});

// ──────────────────────────────────────────────
// GRILLE & AXES
// ──────────────────────────────────────────────
function buildGrid() {{
  if (!SCENE.showGrid) return;

  const minor = new THREE.GridHelper(200, 200, 0x151b25, 0x151b25);
  minor.material.transparent=true; minor.material.opacity=0.6;
  scene.add(minor);

  const major = new THREE.GridHelper(200, 20, 0x1e2d45, 0x1e2d45);
  major.material.transparent=true; major.material.opacity=1;
  scene.add(major);

  // Origin cross
  const mat = new THREE.LineBasicMaterial({{color:0x21262d}});
  const pts = [new THREE.Vector3(-100,0,0), new THREE.Vector3(100,0,0)];
  scene.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), mat));
}}

if (SCENE.showAxes) scene.add(new THREE.AxesHelper(2));
buildGrid();

// ──────────────────────────────────────────────
// MATÉRIAUX PARTAGÉS
// ──────────────────────────────────────────────
const MAT = {{
  pt:     new THREE.MeshPhongMaterial({{color:0x58a6ff, shininess:80}}),
  ptSel:  new THREE.MeshPhongMaterial({{color:0xffd700, shininess:120, emissive:0x443300}}),
  ptHov:  new THREE.MeshPhongMaterial({{color:0x3fb950, shininess:80}}),
  seg:    new THREE.LineBasicMaterial({{color:0x2d5fa8, linewidth:1.5}}),
  segSel: new THREE.LineBasicMaterial({{color:0xffd700}}),
  bbox:   new THREE.LineBasicMaterial({{color:0xffd700, transparent:true, opacity:0.4}}),
  snap:   new THREE.MeshBasicMaterial({{color:0x3fb950, transparent:true, opacity:0.8}}),
}};

const GEO = {{
  pt: new THREE.SphereGeometry(0.06, 10, 8),
  ptSm: new THREE.SphereGeometry(0.04, 8, 6),
}};

// ──────────────────────────────────────────────
// CONSTRUCTION DE LA SCÈNE
// ──────────────────────────────────────────────
const objGroups = {{}};

function buildScene(data) {{
  Object.values(objGroups).forEach(g => scene.remove(g));
  Object.keys(objGroups).forEach(k => delete objGroups[k]);

  data.objects.forEach(obj => {{
    const g = new THREE.Group();
    g.position.set(obj.pos.x*US, obj.pos.y*US, obj.pos.z*US);
    g.quaternion.set(obj.rot.x, obj.rot.y, obj.rot.z, obj.rot.w);
    g.scale.set(obj.scl.x, obj.scl.y, obj.scl.z);
    g.userData = {{type:'object', id:obj.id, name:obj.name}};

    // ── Points ───────────────────────────────
    const ptMap = {{}};
    obj.points.forEach(p => {{
      ptMap[p.id] = p;
      const mat = p.sel ? MAT.ptSel : (obj.sel ? MAT.ptHov : MAT.pt);
      const m   = new THREE.Mesh(GEO.pt, mat);
      m.position.set(p.x*US, p.y*US, p.z*US);
      m.userData = {{type:'point', id:p.id, oid:obj.id}};
      g.add(m);
    }});

    // ── Segments ─────────────────────────────
    obj.segments.forEach(s => {{
      const pa = ptMap[s.a], pb = ptMap[s.b];
      if (!pa || !pb) return;
      const pts = [
        new THREE.Vector3(pa.x*US, pa.y*US, pa.z*US),
        new THREE.Vector3(pb.x*US, pb.y*US, pb.z*US),
      ];
      const line = new THREE.Line(
        new THREE.BufferGeometry().setFromPoints(pts),
        MAT.seg
      );
      line.userData = {{type:'segment', id:s.id, oid:obj.id}};
      g.add(line);
    }});

    // ── Bounding box si sélectionné ──────────
    if (obj.sel && obj.points.length>0) {{
      const bb = new THREE.Box3();
      obj.points.forEach(p => bb.expandByPoint(new THREE.Vector3(p.x*US,p.y*US,p.z*US)));
      bb.min.subScalar(0.05); bb.max.addScalar(0.05);
      g.add(new THREE.Box3Helper(bb, 0xffd700));
    }}

    objGroups[obj.id] = g;
    scene.add(g);
  }});
}}

buildScene(SCENE);

// ──────────────────────────────────────────────
// COORDONNÉES CURSEUR
// ──────────────────────────────────────────────
const ray   = new THREE.Raycaster();
const gndPl = new THREE.Plane(new THREE.Vector3(0,1,0), 0);
const coordDiv = document.getElementById('coords');

function updateCoords(e) {{
  const rect = cv.getBoundingClientRect();
  const m = new THREE.Vector2(
    ((e.clientX-rect.left)/W)*2-1,
    -((e.clientY-rect.top)/H)*2+1
  );
  ray.setFromCamera(m, camera);
  const hit = new THREE.Vector3();
  if (ray.ray.intersectPlane(gndPl, hit)) {{
    coordDiv.textContent =
      `X: ${{(hit.x/US).toFixed(1)}} · Y: ${{(hit.y/US).toFixed(1)}} · Z: ${{(hit.z/US).toFixed(1)}} cm`;
  }}
}}

// ──────────────────────────────────────────────
// SÉLECTION PAR CLIC
// ──────────────────────────────────────────────
const pickRay = new THREE.Raycaster();
pickRay.params.Line.threshold = 0.05;

cv.addEventListener('click', e => {{
  if (isRD) return;
  const rect = cv.getBoundingClientRect();
  const m = new THREE.Vector2(
    ((e.clientX-rect.left)/W)*2-1,
    -((e.clientY-rect.top)/H)*2+1
  );
  pickRay.setFromCamera(m, camera);

  const targets = [];
  Object.values(objGroups).forEach(g => g.traverse(c => {{
    if (c.userData && c.userData.type) targets.push(c);
  }}));

  const hits = pickRay.intersectObjects(targets, false);
  if (hits.length>0) {{
    const ud = hits[0].object.userData;
    window.parent.postMessage(
      {{src:'3ds', evt:'select', payload: ud, multi: e.shiftKey}}, '*'
    );
    setStatus(`Selected: ${{ud.type}} #${{ud.id}}`);
  }} else {{
    window.parent.postMessage({{src:'3ds', evt:'deselect'}}, '*');
    setStatus('Ready');
  }}
}});

// ──────────────────────────────────────────────
// SNAP VISUEL (plan editor)
// ──────────────────────────────────────────────
if (SCENE.snap) {{
  // Indicateurs snap (points bleus) — lecture seule, aucune modif données
  const snapSphere = new THREE.Mesh(
    new THREE.SphereGeometry(0.09, 10, 8),
    MAT.snap
  );
  snapSphere.visible = false;
  scene.add(snapSphere);

  const allPoints = [];
  SCENE.objects.forEach(o => o.points.forEach(p => {{
    allPoints.push({{
      world: new THREE.Vector3(
        (o.pos.x+p.x)*US, (o.pos.y+p.y)*US, (o.pos.z+p.z)*US
      ),
      id: p.id, oid: o.id
    }});
  }}));

  window.addEventListener('mousemove', e => {{
    const rect = cv.getBoundingClientRect();
    const m = new THREE.Vector2(
      ((e.clientX-rect.left)/W)*2-1,
      -((e.clientY-rect.top)/H)*2+1
    );
    const ray2 = new THREE.Raycaster();
    ray2.setFromCamera(m, camera);
    const hit = new THREE.Vector3();
    ray2.ray.intersectPlane(gndPl, hit);

    let nearest = null, minD = SCENE.snapDist * US;
    allPoints.forEach(ap => {{
      const d = hit.distanceTo(ap.world);
      if (d < minD) {{ minD=d; nearest=ap; }}
    }});

    if (nearest) {{
      snapSphere.position.copy(nearest.world);
      snapSphere.visible = true;
    }} else {{
      snapSphere.visible = false;
    }}
  }});
}}

// ──────────────────────────────────────────────
// CLAVIER
// ──────────────────────────────────────────────
const statusDiv = document.getElementById('status');
function setStatus(txt) {{ statusDiv.textContent = txt; }}

window.addEventListener('keydown', e => {{
  const ctrl  = e.ctrlKey;
  const shift = e.shiftKey;
  const step  = ctrl ? 0.1 : (shift ? 10 : 1);  // cm

  const moves = {{
    ArrowLeft:  {{dx:-step, dy:0,    dz:0}},
    ArrowRight: {{dx:step,  dy:0,    dz:0}},
    ArrowUp:    {{dx:0,     dy:step, dz:0}},
    ArrowDown:  {{dx:0,     dy:-step,dz:0}},
  }};

  if (moves[e.key]) {{
    e.preventDefault();
    window.parent.postMessage(
      {{src:'3ds', evt:'move', payload:{{...moves[e.key], step, mode:MODE}}}}, '*'
    );
    setStatus(`Move ${{JSON.stringify(moves[e.key])}} (step ${{step}} cm)`);
  }}

  const modes = {{ r:'rotate', s:'scale', e:'segment' }};
  if (modes[e.key.toLowerCase()]) {{
    setStatus(`Mode: ${{modes[e.key.toLowerCase()]}}`);
    window.parent.postMessage(
      {{src:'3ds', evt:'tool', payload:{{tool:modes[e.key.toLowerCase()]}}}}, '*'
    );
  }}
}});

// ──────────────────────────────────────────────
// RENDER LOOP
// ──────────────────────────────────────────────
let frame = 0;
function animate() {{
  requestAnimationFrame(animate);
  frame++;
  renderer.render(scene, camera);
}}
animate();

// ──────────────────────────────────────────────
// RESIZE
// ──────────────────────────────────────────────
const ro = new ResizeObserver(() => {{
  const nw = wrap.clientWidth;
  renderer.setSize(nw, {height});
  camera.aspect = nw/{height};
  camera.updateProjectionMatrix();
}});
ro.observe(wrap);
</script>
</body>
</html>"""

    st.markdown('<div class="viewer-wrap">', unsafe_allow_html=True)
    st_html(html, height=height + 4, scrolling=False)
    st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS EULER ↔ QUATERNION
# ─────────────────────────────────────────────────────────────────────────────

def quat_to_euler(qx, qy, qz, qw):
    """Quaternion → (ex, ey, ez) degrés"""
    sinr = 2*(qw*qx + qy*qz); cosr = 1 - 2*(qx*qx + qy*qy)
    ex   = math.degrees(math.atan2(sinr, cosr))
    sinp = 2*(qw*qy - qz*qx)
    ey   = math.degrees(math.asin(max(-1, min(1, sinp))))
    siny = 2*(qw*qz + qx*qy); cosy = 1 - 2*(qy*qy + qz*qz)
    ez   = math.degrees(math.atan2(siny, cosy))
    return ex, ey, ez


def euler_to_quat(ex, ey, ez):
    """Degrés (XYZ) → quaternion (qx, qy, qz, qw)"""
    rx, ry, rz = math.radians(ex), math.radians(ey), math.radians(ez)
    cy, sy = math.cos(rz/2), math.sin(rz/2)
    cp, sp = math.cos(ry/2), math.sin(ry/2)
    cr, sr = math.cos(rx/2), math.sin(rx/2)
    return (
        sr*cp*cy - cr*sp*sy,
        cr*sp*cy + sr*cp*sy,
        cr*cp*sy - sr*sp*cy,
        cr*cp*cy + sr*sp*sy,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PANNEAU PLAN EDITOR — TRANSFORMS
# ─────────────────────────────────────────────────────────────────────────────

def panel_plan_editor(
    objects_df: pd.DataFrame,
    points_df:  pd.DataFrame,
    segments_df: pd.DataFrame,
    sel_oid: int | None,
) -> None:

    if sel_oid is None:
        st.markdown(
            '<div class="info-box">👆 Sélectionnez un objet dans le panneau gauche pour éditer ses transformations.<br>'
            '<code>Flèches</code> déplacer &nbsp; <code>R</code> rotation &nbsp; <code>S</code> échelle</div>',
            unsafe_allow_html=True,
        )
        return

    obj_row = objects_df[objects_df["object_id"] == sel_oid] if not objects_df.empty else pd.DataFrame()
    if obj_row.empty:
        return
    obj = obj_row.iloc[0]

    # Stats rapides
    n_pts  = len(points_df[points_df["object_id"] == sel_oid])   if not points_df.empty   else 0
    n_segs = len(segments_df[segments_df["object_id"] == sel_oid]) if not segments_df.empty else 0

    st.markdown(
        f'<div class="metric-row">'
        f'<div class="metric-card"><div class="metric-val">{n_pts}</div><div class="metric-lbl">Points</div></div>'
        f'<div class="metric-card"><div class="metric-val">{n_segs}</div><div class="metric-lbl">Segments</div></div>'
        f'<div class="metric-card"><div class="metric-val">'
        f'{float(obj["scale_x"]):.2f}'
        f'</div><div class="metric-lbl">Scale X</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    tab_pos, tab_rot, tab_scl, tab_del = st.tabs(
        ["📍 Position", "🔄 Rotation", "📐 Échelle", "🗑 Supprimer"]
    )

    with tab_pos:
        c1, c2, c3 = st.columns(3)
        px = c1.number_input("X (cm)", value=float(obj["pos_x"]), step=1.0, format="%.1f", key=f"px_{sel_oid}")
        py = c2.number_input("Y (cm)", value=float(obj["pos_y"]), step=1.0, format="%.1f", key=f"py_{sel_oid}")
        pz = c3.number_input("Z (cm)", value=float(obj["pos_z"]), step=1.0, format="%.1f", key=f"pz_{sel_oid}")
        if st.button("Appliquer position", key="apply_pos"):
            idx = objects_df.index[objects_df["object_id"] == sel_oid][0]
            objects_df.at[idx, "pos_x"] = px
            objects_df.at[idx, "pos_y"] = py
            objects_df.at[idx, "pos_z"] = pz
            save_parquet(objects_df, OBJ_KEY)
            st.rerun()

    with tab_rot:
        ex, ey, ez = quat_to_euler(
            float(obj["rot_x"]), float(obj["rot_y"]),
            float(obj["rot_z"]), float(obj["rot_w"]),
        )
        c1, c2, c3 = st.columns(3)
        rx = c1.number_input("X°", value=round(ex, 2), step=1.0, format="%.2f", key=f"rx_{sel_oid}")
        ry = c2.number_input("Y°", value=round(ey, 2), step=1.0, format="%.2f", key=f"ry_{sel_oid}")
        rz = c3.number_input("Z°", value=round(ez, 2), step=1.0, format="%.2f", key=f"rz_{sel_oid}")
        if st.button("Appliquer rotation", key="apply_rot"):
            qx, qy, qz, qw = euler_to_quat(rx, ry, rz)
            idx = objects_df.index[objects_df["object_id"] == sel_oid][0]
            objects_df.at[idx, "rot_x"] = qx
            objects_df.at[idx, "rot_y"] = qy
            objects_df.at[idx, "rot_z"] = qz
            objects_df.at[idx, "rot_w"] = qw
            save_parquet(objects_df, OBJ_KEY)
            st.rerun()

    with tab_scl:
        uniform = st.checkbox("Échelle uniforme", value=True, key="unif_scl")
        c1, c2, c3 = st.columns(3)
        sx = c1.number_input("X", value=float(obj["scale_x"]), step=0.1, min_value=0.01, format="%.2f", key=f"sx_{sel_oid}")
        sy = c2.number_input("Y", value=float(obj["scale_y"]), step=0.1, min_value=0.01, format="%.2f", key=f"sy_{sel_oid}", disabled=uniform)
        sz = c3.number_input("Z", value=float(obj["scale_z"]), step=0.1, min_value=0.01, format="%.2f", key=f"sz_{sel_oid}", disabled=uniform)
        if st.button("Appliquer échelle", key="apply_scl"):
            idx = objects_df.index[objects_df["object_id"] == sel_oid][0]
            if uniform:
                objects_df.at[idx, "scale_x"] = sx
                objects_df.at[idx, "scale_y"] = sx
                objects_df.at[idx, "scale_z"] = sx
            else:
                objects_df.at[idx, "scale_x"] = sx
                objects_df.at[idx, "scale_y"] = sy
                objects_df.at[idx, "scale_z"] = sz
            save_parquet(objects_df, OBJ_KEY)
            st.rerun()

        # Scaling interne des points
        st.divider()
        st.markdown('<p class="section-label">Scaling interne des points</p>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        int_mult = c1.number_input("Multiplicateur", value=1.0, step=0.1, min_value=0.01, key="int_mult")
        int_cx   = c2.number_input("Centre X (cm)",  value=0.0, step=1.0, key="int_cx")
        if st.button("Appliquer scaling interne", key="apply_int_scl"):
            if not points_df.empty:
                mask = points_df["object_id"] == sel_oid
                if mask.any():
                    points_df.loc[mask, "x"] = (points_df.loc[mask, "x"] - int_cx) * int_mult + int_cx
                    points_df.loc[mask, "y"] = points_df.loc[mask, "y"] * int_mult
                    points_df.loc[mask, "z"] = points_df.loc[mask, "z"] * int_mult
                    save_parquet(points_df, PTS_KEY)
                    st.success("Points re-scalés !")
                    st.rerun()

    with tab_del:
        st.warning(f"⚠️ Supprimer **{obj['name']}** et tous ses points / segments ?")
        if st.button("🗑 Confirmer la suppression", key="confirm_del_obj"):
            objects_df  = objects_df[objects_df["object_id"] != sel_oid]
            if not points_df.empty:
                points_df   = points_df[points_df["object_id"] != sel_oid]
            if not segments_df.empty:
                segments_df = segments_df[segments_df["object_id"] != sel_oid]
            save_parquet(objects_df,  OBJ_KEY)
            save_parquet(points_df,   PTS_KEY)
            save_parquet(segments_df, SEG_KEY)
            st.session_state["object_id"] = None
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PANNEAU OBJECT DESIGNER — POINTS & SEGMENTS
# ─────────────────────────────────────────────────────────────────────────────

def panel_object_designer(
    objects_df:  pd.DataFrame,
    points_df:   pd.DataFrame,
    segments_df: pd.DataFrame,
    sel_oid: int | None,
) -> None:

    if sel_oid is None:
        st.markdown(
            '<div class="info-box">👆 Sélectionnez un objet pour éditer ses points.<br>'
            '<code>Clic G</code> sélect. point &nbsp; <code>E</code> mode segment &nbsp; '
            '<code>G</code> règle &nbsp; <code>H</code> équerre</div>',
            unsafe_allow_html=True,
        )
        return

    obj_row = objects_df[objects_df["object_id"] == sel_oid] if not objects_df.empty else pd.DataFrame()
    if obj_row.empty:
        return
    obj = obj_row.iloc[0]

    o_pts  = points_df[points_df["object_id"]   == sel_oid] if not points_df.empty   else pd.DataFrame()
    o_segs = segments_df[segments_df["object_id"] == sel_oid] if not segments_df.empty else pd.DataFrame()

    tab_pts, tab_segs, tab_import = st.tabs(["📍 Points", "🔗 Segments", "⬇ Import"])

    # ── Points ────────────────────────────────────────────────────────────────
    with tab_pts:
        st.markdown("#### Ajouter un point")
        c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
        nx = c1.number_input("X (cm)", value=0.0, step=1.0, format="%.1f", key="np_x")
        ny = c2.number_input("Y (cm)", value=0.0, step=1.0, format="%.1f", key="np_y")
        nz = c3.number_input("Z (cm)", value=0.0, step=1.0, format="%.1f", key="np_z")
        c4.markdown("<br>", unsafe_allow_html=True)
        if c4.button("➕", key="add_pt"):
            pid = next_id(points_df, "point_id")
            new_row = pd.DataFrame([{
                "point_id": pid, "object_id": sel_oid,
                "x": float(nx), "y": float(ny), "z": float(nz),
            }])
            points_df = pd.concat([points_df, new_row], ignore_index=True)
            save_parquet(points_df, PTS_KEY)
            st.rerun()

        if not o_pts.empty:
            st.markdown(f"**{len(o_pts)} point(s)**")
            edit = st.data_editor(
                o_pts[["point_id", "x", "y", "z"]].reset_index(drop=True),
                key=f"pts_edit_{sel_oid}",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "point_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                    "x": st.column_config.NumberColumn("X (cm)", step=0.1, format="%.1f"),
                    "y": st.column_config.NumberColumn("Y (cm)", step=0.1, format="%.1f"),
                    "z": st.column_config.NumberColumn("Z (cm)", step=0.1, format="%.1f"),
                },
            )

            c1, c2 = st.columns(2)
            if c1.button("💾 Sauvegarder", key="save_pts"):
                for _, row in edit.iterrows():
                    if pd.notna(row.get("point_id")):
                        mask = points_df["point_id"] == int(row["point_id"])
                        if mask.any():
                            idx = points_df.index[mask][0]
                            points_df.at[idx, "x"] = float(row["x"])
                            points_df.at[idx, "y"] = float(row["y"])
                            points_df.at[idx, "z"] = float(row["z"])
                save_parquet(points_df, PTS_KEY)
                st.success("Points sauvegardés")
                st.rerun()

            # Supprimer point
            pt_opts = {f"Pt {int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})": int(r["point_id"]) for _, r in o_pts.iterrows()}
            del_pt_lbl = c2.selectbox("Supprimer", list(pt_opts.keys()), key="del_pt_sel", label_visibility="collapsed")
            if c2.button("🗑 Point", key="del_pt"):
                del_pid = pt_opts[del_pt_lbl]
                points_df   = points_df[points_df["point_id"] != del_pid]
                # Nettoyer segments liés
                if not segments_df.empty:
                    segments_df = segments_df[
                        (segments_df["point_a_id"] != del_pid) &
                        (segments_df["point_b_id"] != del_pid)
                    ]
                save_parquet(points_df,   PTS_KEY)
                save_parquet(segments_df, SEG_KEY)
                st.rerun()
        else:
            st.info("Aucun point. Ajoutez-en un ci-dessus.")

    # ── Segments ──────────────────────────────────────────────────────────────
    with tab_segs:
        if o_pts.empty or len(o_pts) < 2:
            st.info("Il faut au moins 2 points pour créer un segment.")
        else:
            pt_opts = {
                f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})": int(r["point_id"])
                for _, r in o_pts.iterrows()
            }
            labels = list(pt_opts.keys())
            c1, c2 = st.columns(2)
            sa = c1.selectbox("Point A", labels, key="seg_a")
            sb = c2.selectbox("Point B", labels, key="seg_b", index=min(1, len(labels)-1))

            if st.button("🔗 Créer segment", key="mk_seg"):
                pa_id, pb_id = pt_opts[sa], pt_opts[sb]
                if pa_id == pb_id:
                    st.error("Les deux points doivent être différents.")
                else:
                    # Vérifier doublon
                    if not segments_df.empty:
                        dupe = segments_df[
                            (segments_df["object_id"] == sel_oid) &
                            (
                                ((segments_df["point_a_id"] == pa_id) & (segments_df["point_b_id"] == pb_id)) |
                                ((segments_df["point_a_id"] == pb_id) & (segments_df["point_b_id"] == pa_id))
                            )
                        ]
                        if not dupe.empty:
                            st.warning("Ce segment existe déjà.")
                        else:
                            sid = next_id(segments_df, "segment_id")
                            new_seg = pd.DataFrame([{"segment_id": sid, "object_id": sel_oid, "point_a_id": pa_id, "point_b_id": pb_id}])
                            segments_df = pd.concat([segments_df, new_seg], ignore_index=True)
                            save_parquet(segments_df, SEG_KEY)
                            st.rerun()
                    else:
                        sid = next_id(segments_df, "segment_id")
                        new_seg = pd.DataFrame([{"segment_id": sid, "object_id": sel_oid, "point_a_id": pa_id, "point_b_id": pb_id}])
                        segments_df = pd.concat([segments_df, new_seg], ignore_index=True)
                        save_parquet(segments_df, SEG_KEY)
                        st.rerun()

        if not o_segs.empty:
            st.markdown(f"**{len(o_segs)} segment(s)**")
            st.dataframe(
                o_segs[["segment_id", "point_a_id", "point_b_id"]].reset_index(drop=True),
                use_container_width=True, hide_index=True,
                column_config={
                    "segment_id": st.column_config.NumberColumn("ID", width="small"),
                    "point_a_id": st.column_config.NumberColumn("Pt A"),
                    "point_b_id": st.column_config.NumberColumn("Pt B"),
                },
            )
            seg_ids = o_segs["segment_id"].tolist()
            c1, c2 = st.columns([3, 1])
            del_sid = c1.selectbox("Segment à supprimer", seg_ids, key="del_seg_sel")
            if c2.button("🗑", key="del_seg"):
                segments_df = segments_df[segments_df["segment_id"] != del_sid]
                save_parquet(segments_df, SEG_KEY)
                st.rerun()

    # ── Import CSV ─────────────────────────────────────────────────────────────
    with tab_import:
        st.markdown(
            '<div class="info-box">Format CSV attendu :<br>'
            '<code>x,y,z</code> — une ligne par point (en cm)</div>',
            unsafe_allow_html=True,
        )
        uploaded = st.file_uploader("Importer des points (CSV)", type=["csv"], key="csv_import")
        if uploaded:
            try:
                df_csv = pd.read_csv(uploaded, names=["x", "y", "z"])
                st.dataframe(df_csv.head(10), use_container_width=True)
                st.markdown(f"**{len(df_csv)} points** détectés")
                if st.button("⬇ Importer dans l'objet", key="do_import"):
                    base_id = next_id(points_df, "point_id")
                    new_pts = []
                    for i, row in df_csv.iterrows():
                        new_pts.append({
                            "point_id": base_id + i,
                            "object_id": sel_oid,
                            "x": float(row["x"]),
                            "y": float(row["y"]),
                            "z": float(row["z"]),
                        })
                    points_df = pd.concat([points_df, pd.DataFrame(new_pts)], ignore_index=True)
                    save_parquet(points_df, PTS_KEY)
                    st.success(f"{len(new_pts)} points importés !")
                    st.rerun()
            except Exception as exc:
                st.error(f"Erreur de lecture CSV : {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# APPLICATION PRINCIPALE
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    init_session()

    # Init R2 tables une seule fois
    if not st.session_state["r2_ready"]:
        try:
            init_r2_tables()
            st.session_state["r2_ready"] = True
        except Exception as exc:
            st.warning(f"R2 non disponible : {exc}. Vérifiez vos secrets.")

    # Chargement des données
    proj_df = load_parquet(PROJ_KEY, PROJ_COLS)
    obj_df  = load_parquet(OBJ_KEY,  OBJ_COLS)
    pts_df  = load_parquet(PTS_KEY,  PTS_COLS)
    seg_df  = load_parquet(SEG_KEY,  SEG_COLS)

    # ─── SIDEBAR ─────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(
            '<div class="studio-header">'
            '<div><div class="studio-title">🧊 3D Design Studio</div>'
            '<div class="studio-sub">Point · Segment · Transform</div></div>'
            '</div>',
            unsafe_allow_html=True,
        )

        # Mode
        mode_lbl = st.radio(
            "Mode d'édition",
            ["📐 Plan Editor", "✏️ Object Designer"],
            index=0 if st.session_state["mode"] == "plan_editor" else 1,
            horizontal=True,
            label_visibility="collapsed",
        )
        st.session_state["mode"] = "plan_editor" if "Plan" in mode_lbl else "object_designer"

        badge_cls = "badge-plan" if st.session_state["mode"] == "plan_editor" else "badge-object"
        badge_lbl = "PLAN EDITOR" if st.session_state["mode"] == "plan_editor" else "OBJECT DESIGNER"
        st.markdown(
            f'<span class="badge {badge_cls}">{badge_lbl}</span>',
            unsafe_allow_html=True,
        )

        st.divider()

        # ── Projets ──────────────────────────────────────────────────────────
        st.markdown('<p class="section-label">📁 Projets</p>', unsafe_allow_html=True)

        with st.expander("Nouveau projet", expanded=proj_df.empty):
            pname = st.text_input("Nom du projet", key="new_proj_name", placeholder="Mon projet…")
            if st.button("Créer", key="create_proj"):
                if pname.strip():
                    pid = next_id(proj_df, "project_id")
                    row = pd.DataFrame([{
                        "project_id": pid,
                        "name": pname.strip(),
                        "created_at": datetime.now().isoformat(),
                    }])
                    proj_df = pd.concat([proj_df, row], ignore_index=True)
                    save_parquet(proj_df, PROJ_KEY)
                    st.session_state["project_id"] = pid
                    st.session_state["object_id"]  = None
                    st.rerun()

        if not proj_df.empty:
            pnames = proj_df["name"].tolist()
            pids   = proj_df["project_id"].tolist()
            cur    = st.session_state["project_id"]
            ci     = pids.index(cur) if cur in pids else 0

            sel_pname = st.selectbox("Projet actif", pnames, index=ci, key="proj_sel",
                                     label_visibility="collapsed")
            st.session_state["project_id"] = pids[pnames.index(sel_pname)]

            # Supprimer projet
            if st.button("🗑 Supprimer ce projet", key="del_proj"):
                dpid = st.session_state["project_id"]
                proj_df = proj_df[proj_df["project_id"] != dpid]
                if not obj_df.empty:
                    doids = obj_df[obj_df["project_id"] == dpid]["object_id"].tolist()
                    obj_df  = obj_df[obj_df["project_id"] != dpid]
                    if not pts_df.empty:
                        pts_df  = pts_df[~pts_df["object_id"].isin(doids)]
                    if not seg_df.empty:
                        seg_df  = seg_df[~seg_df["object_id"].isin(doids)]
                save_parquet(proj_df, PROJ_KEY)
                save_parquet(obj_df,  OBJ_KEY)
                save_parquet(pts_df,  PTS_KEY)
                save_parquet(seg_df,  SEG_KEY)
                st.session_state["project_id"] = None
                st.session_state["object_id"]  = None
                st.rerun()
        else:
            st.caption("Aucun projet. Créez-en un.")

        st.divider()

        # ── Objets ───────────────────────────────────────────────────────────
        cur_pid = st.session_state.get("project_id")
        st.markdown('<p class="section-label">📦 Objets</p>', unsafe_allow_html=True)

        if cur_pid is not None:
            with st.expander("Nouvel objet"):
                oname = st.text_input("Nom de l'objet", key="new_obj_name", placeholder="Objet A…")
                if st.button("Créer", key="create_obj"):
                    oid = next_id(obj_df, "object_id")
                    row = pd.DataFrame([{
                        "object_id": oid, "project_id": cur_pid,
                        "name": oname.strip() or f"Objet {oid}",
                        "pos_x":0.0,"pos_y":0.0,"pos_z":0.0,
                        "rot_x":0.0,"rot_y":0.0,"rot_z":0.0,"rot_w":1.0,
                        "scale_x":1.0,"scale_y":1.0,"scale_z":1.0,
                    }])
                    obj_df = pd.concat([obj_df, row], ignore_index=True)
                    save_parquet(obj_df, OBJ_KEY)
                    st.session_state["object_id"] = oid
                    st.rerun()

            proj_objs = obj_df[obj_df["project_id"] == cur_pid] if not obj_df.empty else pd.DataFrame()
            sel_oid   = st.session_state.get("object_id")

            if not proj_objs.empty:
                for _, o in proj_objs.iterrows():
                    oid   = int(o["object_id"])
                    active = oid == sel_oid
                    n_p   = len(pts_df[pts_df["object_id"] == oid]) if not pts_df.empty else 0
                    lbl   = f"{'▶ ' if active else '  '}{o['name']}  ·  {n_p}pt"
                    if st.button(lbl, key=f"sel_obj_{oid}", use_container_width=True):
                        st.session_state["object_id"] = oid
                        st.rerun()
            else:
                st.caption("Aucun objet dans ce projet.")
        else:
            st.caption("Sélectionnez un projet.")

        st.divider()

        # ── Affichage ─────────────────────────────────────────────────────────
        st.markdown('<p class="section-label">👁 Affichage</p>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        st.session_state["show_grid"] = c1.checkbox("Grille", value=True)
        st.session_state["show_axes"] = c2.checkbox("Axes",   value=True)
        st.session_state["snap"]      = st.checkbox("Snap visuel", value=True)
        if st.session_state["snap"]:
            st.session_state["snap_dist"] = st.slider(
                "Seuil snap (cm)", 0.5, 30.0, 5.0, 0.5,
                label_visibility="collapsed",
            )

    # ─── ZONE PRINCIPALE ─────────────────────────────────────────────────────
    cur_oid = st.session_state.get("object_id")
    cur_pts = st.session_state.get("selected_pts", [])

    scene_data = _build_scene_json(
        cur_pid, obj_df, pts_df, seg_df, cur_oid, cur_pts
    )

    render_viewer(scene_data, st.session_state["mode"], height=520)

    # ─── PANNEAU PROPRIÉTÉS ──────────────────────────────────────────────────
    st.markdown("<hr style='border-color:#21262d;margin:8px 0'>", unsafe_allow_html=True)

    if st.session_state["mode"] == "plan_editor":
        panel_plan_editor(obj_df, pts_df, seg_df, cur_oid)
    else:
        panel_object_designer(obj_df, pts_df, seg_df, cur_oid)


if __name__ == "__main__":
    main()
