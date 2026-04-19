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
    html, body, [class*="css"] { font-family: 'JetBrains Mono', monospace; }
    :root {
        --bg0:#0a0c10; --bg1:#0f1117; --bg2:#161b22; --bg3:#1c2333;
        --border:#21262d; --accent:#1a73e8; --accent2:#2e7d32; --accent3:#f78166;
        --text0:#e6edf3; --text1:#8b949e; --text2:#484f58;
    }
    .stApp { background: var(--bg0); }
    section[data-testid="stSidebar"] {
        background: var(--bg1) !important; border-right: 1px solid var(--border);
    }
    section[data-testid="stSidebar"] > div { padding-top: 0.5rem; }
    .main .block-container { padding: 0.75rem 1rem 1rem 1rem; max-width: 100%; }

    .studio-header {
        display:flex; align-items:center; gap:10px;
        padding:12px 0 8px 0; border-bottom:1px solid var(--border); margin-bottom:12px;
    }
    .studio-title {
        font-family:'Syne',sans-serif; font-size:18px; font-weight:800;
        color:var(--accent); letter-spacing:-0.5px;
    }
    .studio-sub { font-size:9px; color:var(--text2); letter-spacing:2px; text-transform:uppercase; }

    .badge {
        display:inline-block; padding:2px 8px; border-radius:4px;
        font-size:10px; font-weight:600; letter-spacing:1px; text-transform:uppercase;
    }
    .badge-plan   { background:#1a2744; color:#58a6ff; border:1px solid #1f3a72; }
    .badge-object { background:#2a1a1a; color:#f78166; border:1px solid #5a2a2a; }

    .section-label {
        font-size:9px; letter-spacing:2px; text-transform:uppercase;
        color:var(--text2); margin:8px 0 4px 0;
    }

    .metric-row { display:grid; grid-template-columns:repeat(3,1fr); gap:6px; margin:8px 0; }
    .metric-card {
        background:var(--bg2); border:1px solid var(--border);
        border-radius:6px; padding:8px 10px; text-align:center;
    }
    .metric-val { font-size:18px; font-weight:700; color:var(--accent); }
    .metric-lbl { font-size:9px; color:var(--text2); letter-spacing:1px; text-transform:uppercase; }

    .pos-display {
        background:var(--bg2); border:1px solid var(--border);
        border-radius:6px; padding:8px 12px; margin:6px 0;
        display:flex; gap:16px; align-items:center; flex-wrap:wrap;
    }
    .pos-axis { font-size:11px; }
    .pos-axis span { color:var(--text2); font-size:9px; text-transform:uppercase; margin-right:3px; }

    .move-lbl {
        font-size:9px; color:var(--text2); letter-spacing:1.5px;
        text-transform:uppercase; margin-bottom:3px; margin-top:8px;
    }

    .info-box {
        background:var(--bg2); border:1px solid var(--border);
        border-radius:6px; padding:10px 12px; font-size:11px; color:var(--text1); margin:8px 0;
    }
    .info-box code { background:var(--bg3); padding:1px 4px; border-radius:3px; color:#3fb950; font-size:10px; }

    .viewer-wrap { border-radius:8px; overflow:hidden; border:1px solid var(--border); background:#fff; }

    .stButton > button {
        background:var(--bg2) !important; border:1px solid var(--border) !important;
        color:var(--text0) !important; font-family:'JetBrains Mono',monospace !important;
        font-size:11px !important; border-radius:5px !important; transition:all .15s ease !important;
    }
    .stButton > button:hover { border-color:var(--accent) !important; color:var(--accent) !important; }

    .stTabs [data-baseweb="tab"] { font-family:'JetBrains Mono',monospace; font-size:11px; }

    div[data-testid="stNumberInput"] input {
        font-family:'JetBrains Mono',monospace; font-size:12px;
        background:var(--bg2) !important; border-color:var(--border) !important; color:var(--text0) !important;
    }
    div[data-testid="stDataFrame"] { font-size:11px; }
    .stAlert { font-size:11px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# R2 / PARQUET
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


def load_parquet(key: str, cols: list) -> pd.DataFrame:
    try:
        obj = get_r2().get_object(Bucket=st.secrets["R2_BUCKET"], Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        return pd.DataFrame(columns=cols)


def save_parquet(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="zstd")
    buf.seek(0)
    get_r2().put_object(Bucket=st.secrets["R2_BUCKET"], Key=key, Body=buf.getvalue())


PROJ_KEY  = "projects.parquet"
OBJ_KEY   = "objects.parquet"
PTS_KEY   = "points.parquet"
SEG_KEY   = "segments.parquet"

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


def init_r2_tables():
    for key, cols in [
        (PROJ_KEY, PROJ_COLS), (OBJ_KEY, OBJ_COLS),
        (PTS_KEY, PTS_COLS),   (SEG_KEY, SEG_COLS),
    ]:
        if load_parquet(key, cols).empty:
            try:
                save_parquet(pd.DataFrame(columns=cols), key)
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────

def _ss(k, v):
    if k not in st.session_state:
        st.session_state[k] = v


def init_session():
    _ss("mode",         "plan_editor")
    _ss("project_id",   None)
    _ss("object_id",    None)
    _ss("selected_pts", [])
    _ss("show_grid",    True)
    _ss("show_axes",    True)
    _ss("snap",         True)
    _ss("snap_dist",    5.0)
    _ss("r2_ready",     False)
    _ss("move_step",    1.0)
    _ss("rot_step",     5.0)
    _ss("scale_step",   0.1)
    _ss("pt_move_step", 1.0)


# ─────────────────────────────────────────────────────────────────────────────
# MATH HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def quat_to_euler(qx, qy, qz, qw):
    sinr = 2*(qw*qx + qy*qz); cosr = 1 - 2*(qx*qx + qy*qy)
    ex   = math.degrees(math.atan2(sinr, cosr))
    sinp = 2*(qw*qy - qz*qx)
    ey   = math.degrees(math.asin(max(-1, min(1, sinp))))
    siny = 2*(qw*qz + qx*qy); cosy = 1 - 2*(qy*qy + qz*qz)
    ez   = math.degrees(math.atan2(siny, cosy))
    return ex, ey, ez


def euler_to_quat(ex, ey, ez):
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


def compose_rot(qx, qy, qz, qw, axis: str, deg: float):
    """Applique une rotation delta (axe mondial) sur un quaternion."""
    a = math.radians(deg) / 2
    c, s = math.cos(a), math.sin(a)
    dq = {"x": (s,0,0,c), "y": (0,s,0,c), "z": (0,0,s,c)}[axis]
    dx, dy, dz, dw = dq
    nx = dw*qx + dx*qw + dy*qz - dz*qy
    ny = dw*qy - dx*qz + dy*qw + dz*qx
    nz = dw*qz + dx*qy - dy*qx + dz*qw
    nw = dw*qw - dx*qx - dy*qy - dz*qz
    return nx, ny, nz, nw


# ─────────────────────────────────────────────────────────────────────────────
# SCENE JSON
# ─────────────────────────────────────────────────────────────────────────────

def build_scene_json(project_id, obj_df, pts_df, seg_df, sel_obj, sel_pts) -> dict:
    scene = {
        "objects":  [],
        "showGrid": st.session_state["show_grid"],
        "showAxes": st.session_state["show_axes"],
        "snap":     st.session_state["snap"],
        "snapDist": st.session_state["snap_dist"],
        "unitScale": 0.01,
        "mode":     st.session_state["mode"],
    }
    if project_id is None or obj_df.empty:
        return scene

    for _, obj in obj_df[obj_df["project_id"] == project_id].iterrows():
        oid   = int(obj["object_id"])
        o_pts = pts_df[pts_df["object_id"] == oid] if not pts_df.empty else pd.DataFrame()
        o_seg = seg_df[seg_df["object_id"]  == oid] if not seg_df.empty else pd.DataFrame()

        pts  = [{"id": int(p["point_id"]), "x": float(p["x"]), "y": float(p["y"]),
                 "z": float(p["z"]), "sel": int(p["point_id"]) in sel_pts}
                for _, p in o_pts.iterrows()]
        segs = [{"id": int(s["segment_id"]), "a": int(s["point_a_id"]), "b": int(s["point_b_id"])}
                for _, s in o_seg.iterrows()]

        scene["objects"].append({
            "id": oid, "name": str(obj["name"]),
            "pos": {"x": float(obj["pos_x"]), "y": float(obj["pos_y"]), "z": float(obj["pos_z"])},
            "rot": {"x": float(obj["rot_x"]), "y": float(obj["rot_y"]),
                    "z": float(obj["rot_z"]), "w": float(obj["rot_w"])},
            "scl": {"x": float(obj["scale_x"]), "y": float(obj["scale_y"]), "z": float(obj["scale_z"])},
            "points": pts, "segments": segs, "sel": oid == sel_obj,
        })
    return scene


# ─────────────────────────────────────────────────────────────────────────────
# THREE.JS VIEWER
# ─────────────────────────────────────────────────────────────────────────────

def render_viewer(scene: dict, mode: str, height: int = 520):
    sj       = json.dumps(scene)
    is_plan  = mode == "plan_editor"
    bcls     = "badge-plan" if is_plan else "badge-object"
    blbl     = "PLAN EDITOR" if is_plan else "OBJECT DESIGNER"

    viewer_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
  *{{margin:0;padding:0;box-sizing:border-box;}}
  body{{background:#fff;overflow:hidden;font-family:'JetBrains Mono',monospace;}}
  #wrap{{width:100%;height:{height}px;position:relative;}}
  .hud{{position:absolute;pointer-events:none;font-size:10px;}}
  #badge{{top:10px;left:10px;padding:4px 10px;border-radius:4px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;}}
  .badge-plan{{background:rgba(26,39,68,.88);color:#58a6ff;border:1px solid #1f3a72;}}
  .badge-object{{background:rgba(42,26,26,.88);color:#f78166;border:1px solid #5a2a2a;}}
  #coords{{bottom:10px;left:10px;color:#333;background:rgba(255,255,255,.9);padding:5px 10px;border-radius:4px;border:1px solid #ccc;font-size:11px;}}
  #help{{top:10px;right:10px;color:#555;background:rgba(255,255,255,.9);padding:8px 12px;border-radius:6px;border:1px solid #ccc;line-height:1.9;}}
  #status{{bottom:10px;right:10px;color:#444;background:rgba(255,255,255,.9);padding:5px 10px;border-radius:4px;border:1px solid #ccc;}}
</style>
</head>
<body>
<div id="wrap">
  <div id="badge" class="hud {bcls}">{blbl}</div>
  <div id="help" class="hud">
    🖱 Clic droit + glisser → rotation caméra<br>
    🖱 Molette + glisser → pan<br>
    🖱 Molette → zoom<br>
    🖱 Clic gauche → sélectionner objet
  </div>
  <div id="coords" class="hud">X: 0.0 · Y: 0.0 · Z: 0.0 cm</div>
  <div id="status" class="hud">Prêt</div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
const SCENE={sj};
const MODE={json.dumps(mode)};
const US=SCENE.unitScale;

// ── Renderer ──────────────────────────────────────────────────────────────────
const wrap=document.getElementById('wrap');
const W=wrap.clientWidth, H={height};
const renderer=new THREE.WebGLRenderer({{antialias:true}});
renderer.setSize(W,H);
renderer.setPixelRatio(Math.min(window.devicePixelRatio,2));
renderer.setClearColor(0xffffff,1);
wrap.appendChild(renderer.domElement);

const scene=new THREE.Scene();
scene.background=new THREE.Color(0xffffff);

const camera=new THREE.PerspectiveCamera(55,W/H,0.01,5000);
camera.position.set(8,6,12);
camera.lookAt(0,0,0);

// ── Lights ────────────────────────────────────────────────────────────────────
scene.add(new THREE.AmbientLight(0xffffff,1.0));
const dl=new THREE.DirectionalLight(0xffffff,0.3);
dl.position.set(10,20,10);
scene.add(dl);

// ── Orbit ─────────────────────────────────────────────────────────────────────
let sph={{theta:0.6,phi:0.9,r:18}};
let tgt=new THREE.Vector3();
let isRD=false,isMD=false,lm={{x:0,y:0}};

function applyCamera(){{
  const sp=Math.sin(sph.phi),cp=Math.cos(sph.phi);
  const st=Math.sin(sph.theta),ct=Math.cos(sph.theta);
  camera.position.set(tgt.x+sph.r*sp*st, tgt.y+sph.r*cp, tgt.z+sph.r*sp*ct);
  camera.lookAt(tgt);
}}
applyCamera();

const cv=renderer.domElement;
cv.addEventListener('contextmenu',e=>e.preventDefault());
cv.addEventListener('mousedown',e=>{{
  if(e.button===2)isRD=true;
  if(e.button===1){{isMD=true;e.preventDefault();}}
  lm={{x:e.clientX,y:e.clientY}};
}});
window.addEventListener('mouseup',()=>{{isRD=false;isMD=false;}});
window.addEventListener('mousemove',e=>{{
  const dx=e.clientX-lm.x,dy=e.clientY-lm.y;
  lm={{x:e.clientX,y:e.clientY}};
  if(isRD){{
    sph.theta-=dx*0.005;
    sph.phi=Math.max(0.05,Math.min(Math.PI-0.05,sph.phi+dy*0.005));
    applyCamera();
  }}
  if(isMD){{
    const sp=sph.r*0.0008;
    const right=new THREE.Vector3();
    right.crossVectors(camera.getWorldDirection(new THREE.Vector3()),camera.up).normalize();
    tgt.addScaledVector(right,-dx*sp);
    tgt.addScaledVector(camera.up,dy*sp);
    applyCamera();
  }}
  updateCoords(e);
}});
cv.addEventListener('wheel',e=>{{
  e.preventDefault();
  sph.r=Math.max(0.3,Math.min(800,sph.r*(1+e.deltaY*0.001)));
  applyCamera();
}},{{passive:false}});

// ── Grid (gris clair) ─────────────────────────────────────────────────────────
if(SCENE.showGrid){{
  const g1=new THREE.GridHelper(200,200,0xe0e0e0,0xe0e0e0);
  g1.material.transparent=true;g1.material.opacity=0.7;
  scene.add(g1);
  const g2=new THREE.GridHelper(200,20,0xbbbbbb,0xbbbbbb);
  scene.add(g2);
}}

// ── Axes gris ─────────────────────────────────────────────────────────────────
if(SCENE.showAxes){{
  const L=3;
  const mat=new THREE.LineBasicMaterial({{color:0x999999}});
  [[[0,0,0],[L,0,0]],[[0,0,0],[0,L,0]],[[0,0,0],[0,0,L]]].forEach(pts=>{{
    scene.add(new THREE.Line(
      new THREE.BufferGeometry().setFromPoints(pts.map(p=>new THREE.Vector3(...p))),
      mat
    ));
  }});
  // Petites sphères aux extrémités
  const tg=new THREE.SphereGeometry(0.04,6,4);
  const tm=new THREE.MeshBasicMaterial({{color:0xaaaaaa}});
  [[L,0,0],[0,L,0],[0,0,L]].forEach(p=>{{
    const m=new THREE.Mesh(tg,tm);m.position.set(...p);scene.add(m);
  }});
}}

// ── Matériaux ─────────────────────────────────────────────────────────────────
const MAT={{
  pt:    new THREE.MeshPhongMaterial({{color:0x111111,shininess:10}}),
  ptSel: new THREE.MeshPhongMaterial({{color:0xf59e0b,shininess:60,emissive:0x3d2900}}),
  seg:   new THREE.LineBasicMaterial({{color:0x555555}}),
  segSel:new THREE.LineBasicMaterial({{color:0x1a73e8}}),
  snap:  new THREE.MeshBasicMaterial({{color:0x1a73e8,transparent:true,opacity:0.8}}),
}};
const ptGeo=new THREE.SphereGeometry(0.06,10,8);

// ── Build scene ───────────────────────────────────────────────────────────────
const objGroups={{}};

function buildScene(data){{
  Object.values(objGroups).forEach(g=>scene.remove(g));
  Object.keys(objGroups).forEach(k=>delete objGroups[k]);

  data.objects.forEach(obj=>{{
    const g=new THREE.Group();
    g.position.set(obj.pos.x*US,obj.pos.y*US,obj.pos.z*US);
    g.quaternion.set(obj.rot.x,obj.rot.y,obj.rot.z,obj.rot.w);
    g.scale.set(obj.scl.x,obj.scl.y,obj.scl.z);
    g.userData={{type:'object',id:obj.id,name:obj.name}};

    const ptMap={{}};
    obj.points.forEach(p=>{{ ptMap[p.id]=p; }});

    // Points (object designer uniquement)
    if(MODE==='object_designer'){{
      obj.points.forEach(p=>{{
        const mat=p.sel?MAT.ptSel:MAT.pt;
        const m=new THREE.Mesh(ptGeo,mat.clone());
        m.position.set(p.x*US,p.y*US,p.z*US);
        m.userData={{type:'point',id:p.id,oid:obj.id}};
        g.add(m);
      }});
    }}

    // Segments
    obj.segments.forEach(s=>{{
      const pa=ptMap[s.a],pb=ptMap[s.b];
      if(!pa||!pb) return;
      const mat=obj.sel?MAT.segSel:MAT.seg;
      const line=new THREE.Line(
        new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(pa.x*US,pa.y*US,pa.z*US),
          new THREE.Vector3(pb.x*US,pb.y*US,pb.z*US),
        ]),
        mat.clone()
      );
      line.userData={{type:'segment',id:s.id,oid:obj.id}};
      g.add(line);
    }});

    // Sélection : bounding box bleue
    if(obj.sel && obj.points.length>0){{
      const bb=new THREE.Box3();
      obj.points.forEach(p=>bb.expandByPoint(new THREE.Vector3(p.x*US,p.y*US,p.z*US)));
      if(!bb.isEmpty()){{
        bb.min.subScalar(0.1);bb.max.addScalar(0.1);
        g.add(new THREE.Box3Helper(bb,0x1a73e8));
      }}
    }}

    // Plan editor : proxy invisible pour le picking
    if(MODE==='plan_editor'){{
      let bb=new THREE.Box3();
      if(obj.points.length>0){{
        obj.points.forEach(p=>bb.expandByPoint(new THREE.Vector3(p.x*US,p.y*US,p.z*US)));
      }}else{{
        bb.set(new THREE.Vector3(-0.3,-0.3,-0.3),new THREE.Vector3(0.3,0.3,0.3));
      }}
      bb.min.subScalar(0.2);bb.max.addScalar(0.2);
      const sz=new THREE.Vector3();bb.getSize(sz);
      const ct=new THREE.Vector3();bb.getCenter(ct);
      const proxy=new THREE.Mesh(
        new THREE.BoxGeometry(sz.x,sz.y,sz.z),
        new THREE.MeshBasicMaterial({{visible:false,side:THREE.DoubleSide}})
      );
      proxy.position.copy(ct);
      proxy.userData={{type:'object',id:obj.id,name:obj.name}};
      g.add(proxy);
    }}

    objGroups[obj.id]=g;
    scene.add(g);
  }});
}}
buildScene(SCENE);

// ── Snap (object designer) ────────────────────────────────────────────────────
if(SCENE.snap && MODE==='object_designer'){{
  const snapSph=new THREE.Mesh(new THREE.SphereGeometry(0.09,10,8),MAT.snap);
  snapSph.visible=false;
  scene.add(snapSph);
  const allPts=[];
  SCENE.objects.forEach(o=>o.points.forEach(p=>{{
    allPts.push({{
      w:new THREE.Vector3((o.pos.x+p.x)*US,(o.pos.y+p.y)*US,(o.pos.z+p.z)*US),
      id:p.id,oid:o.id
    }});
  }}));
  const gp=new THREE.Plane(new THREE.Vector3(0,1,0),0);
  window.addEventListener('mousemove',e=>{{
    const r=cv.getBoundingClientRect();
    const m=new THREE.Vector2(((e.clientX-r.left)/W)*2-1,-((e.clientY-r.top)/H)*2+1);
    const ray=new THREE.Raycaster();ray.setFromCamera(m,camera);
    const h=new THREE.Vector3();ray.ray.intersectPlane(gp,h);
    let near=null,minD=SCENE.snapDist*US;
    allPts.forEach(ap=>{{const d=h.distanceTo(ap.w);if(d<minD){{minD=d;near=ap;}}}});
    if(near){{snapSph.position.copy(near.w);snapSph.visible=true;}}
    else snapSph.visible=false;
  }});
}}

// ── Coords ────────────────────────────────────────────────────────────────────
const gndPl=new THREE.Plane(new THREE.Vector3(0,1,0),0);
const coordDiv=document.getElementById('coords');
const statusDiv=document.getElementById('status');

function updateCoords(e){{
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((e.clientX-r.left)/W)*2-1,-((e.clientY-r.top)/H)*2+1);
  const ray=new THREE.Raycaster();ray.setFromCamera(m,camera);
  const h=new THREE.Vector3();
  if(ray.ray.intersectPlane(gndPl,h)){{
    coordDiv.textContent=`X: ${{(h.x/US).toFixed(1)}} · Y: ${{(h.y/US).toFixed(1)}} · Z: ${{(h.z/US).toFixed(1)}} cm`;
  }}
}}

// ── Picking ───────────────────────────────────────────────────────────────────
const pickRay=new THREE.Raycaster();
pickRay.params.Line={{threshold:0.06}};

cv.addEventListener('click',e=>{{
  if(isRD) return;
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((e.clientX-r.left)/W)*2-1,-((e.clientY-r.top)/H)*2+1);
  pickRay.setFromCamera(m,camera);
  const tgts=[];
  Object.values(objGroups).forEach(g=>g.traverse(c=>{{if(c.userData&&c.userData.type)tgts.push(c);}}));
  const hits=pickRay.intersectObjects(tgts,false);
  if(hits.length>0){{
    const ud=hits[0].object.userData;
    // Plan editor : on remonte toujours à l'objet parent
    const payload=(MODE==='plan_editor')
      ?{{type:'object',id:ud.oid||ud.id,name:ud.name||''}}
      :ud;
    window.parent.postMessage({{src:'3ds',evt:'select',payload,multi:e.shiftKey}},'*');
    statusDiv.textContent='Sélectionné : '+payload.type+' #'+payload.id;
  }}else{{
    window.parent.postMessage({{src:'3ds',evt:'deselect'}},'*');
    statusDiv.textContent='Prêt';
  }}
}});

// ── Render loop ───────────────────────────────────────────────────────────────
(function loop(){{ requestAnimationFrame(loop); renderer.render(scene,camera); }})();

// ── Resize ────────────────────────────────────────────────────────────────────
new ResizeObserver(()=>{{
  const nw=wrap.clientWidth;
  renderer.setSize(nw,{height});
  camera.aspect=nw/{height};
  camera.updateProjectionMatrix();
}}).observe(wrap);
</script>
</body></html>"""

    st.markdown('<div class="viewer-wrap">', unsafe_allow_html=True)
    st_html(viewer_html, height=height + 4, scrolling=False)
    st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PLAN EDITOR — delta-based, déplacements illimités
# ─────────────────────────────────────────────────────────────────────────────

def _obj_idx(obj_df, oid):
    m = obj_df.index[obj_df["object_id"] == oid]
    return m[0] if len(m) else None


def panel_plan_editor(obj_df, pts_df, seg_df, sel_oid):

    if sel_oid is None:
        st.markdown(
            '<div class="info-box">'
            '👆 Sélectionnez un objet dans la vue 3D (clic gauche) ou dans la liste à gauche.<br><br>'
            '<b>Navigation 3D :</b><br>'
            '• <code>Clic droit + glisser</code> — rotation caméra<br>'
            '• <code>Molette + glisser</code> — déplacer la vue (pan)<br>'
            '• <code>Molette</code> — zoom avant / arrière<br>'
            '• <code>Clic gauche</code> — sélectionner un objet'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    row = obj_df[obj_df["object_id"] == sel_oid]
    if row.empty:
        return
    obj = row.iloc[0]

    px, py, pz = float(obj["pos_x"]), float(obj["pos_y"]), float(obj["pos_z"])
    qx, qy, qz, qw = float(obj["rot_x"]), float(obj["rot_y"]), float(obj["rot_z"]), float(obj["rot_w"])
    sx, sy, sz = float(obj["scale_x"]), float(obj["scale_y"]), float(obj["scale_z"])
    ex, ey, ez = quat_to_euler(qx, qy, qz, qw)

    n_pts  = len(pts_df[pts_df["object_id"] == sel_oid]) if not pts_df.empty else 0
    n_segs = len(seg_df[seg_df["object_id"]  == sel_oid]) if not seg_df.empty else 0

    # En-tête
    st.markdown(
        f'<div class="metric-row">'
        f'<div class="metric-card"><div class="metric-val">{obj["name"]}</div><div class="metric-lbl">Objet</div></div>'
        f'<div class="metric-card"><div class="metric-val">{n_pts}</div><div class="metric-lbl">Points</div></div>'
        f'<div class="metric-card"><div class="metric-val">{n_segs}</div><div class="metric-lbl">Segments</div></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Affichage position/rotation couranteS
    st.markdown(
        f'<div class="pos-display">'
        f'<div class="pos-axis"><span>X</span>{px:.1f} cm</div>'
        f'<div class="pos-axis"><span>Y</span>{py:.1f} cm</div>'
        f'<div class="pos-axis"><span>Z</span>{pz:.1f} cm</div>'
        f'<div class="pos-axis" style="margin-left:10px">'
        f'<span>RX</span>{ex:.1f}° &nbsp;<span>RY</span>{ey:.1f}° &nbsp;<span>RZ</span>{ez:.1f}°'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    t_move, t_rot, t_scl, t_abs, t_del = st.tabs(
        ["🕹 Déplacer", "🔄 Pivoter", "📐 Échelle", "📍 Position exacte", "🗑 Supprimer"]
    )

    # ══════════════════════════════════════════════════════════════════════════
    # DÉPLACER
    # ══════════════════════════════════════════════════════════════════════════
    with t_move:
        c1, c2 = st.columns([3, 1])
        step = c1.number_input(
            "Pas de déplacement (cm)", min_value=0.1, max_value=9999.0,
            value=st.session_state["move_step"], step=0.1, format="%.1f", key="v_move_step"
        )
        st.session_state["move_step"] = step
        c2.markdown(f"<div style='padding-top:28px;font-size:10px;color:#888'>{step:.1f} cm</div>", unsafe_allow_html=True)

        def _mv(dx=0.0, dy=0.0, dz=0.0):
            idx = _obj_idx(obj_df, sel_oid)
            if idx is None: return
            df2 = obj_df.copy()
            df2.at[idx,"pos_x"] += dx
            df2.at[idx,"pos_y"] += dy
            df2.at[idx,"pos_z"] += dz
            save_parquet(df2, OBJ_KEY)
            st.rerun()

        st.markdown('<p class="move-lbl">Plan horizontal  X / Z</p>', unsafe_allow_html=True)

        _, top_c, _ = st.columns([1,1,1])
        if top_c.button("⬆  −Z", key="m_mz", use_container_width=True):
            _mv(dz=-step)

        lc, mc, rc = st.columns(3)
        if lc.button("◀  −X", key="m_mx", use_container_width=True):
            _mv(dx=-step)
        mc.markdown(
            f"<div style='text-align:center;padding:10px 0;border:1px solid #21262d;"
            f"border-radius:5px;font-size:10px;color:#888'>X {px:.1f}<br>Z {pz:.1f}</div>",
            unsafe_allow_html=True,
        )
        if rc.button("▶  +X", key="m_px", use_container_width=True):
            _mv(dx=+step)

        _, bot_c, _ = st.columns([1,1,1])
        if bot_c.button("⬇  +Z", key="m_pz", use_container_width=True):
            _mv(dz=+step)

        st.markdown('<p class="move-lbl">Vertical  Y</p>', unsafe_allow_html=True)
        yc1, yc2, yc3 = st.columns(3)
        if yc1.button("▲  +Y", key="m_py", use_container_width=True):
            _mv(dy=+step)
        yc2.markdown(
            f"<div style='text-align:center;padding:6px 0;font-size:10px;color:#888'>Y {py:.1f} cm</div>",
            unsafe_allow_html=True,
        )
        if yc3.button("▼  −Y", key="m_my", use_container_width=True):
            _mv(dy=-step)

        st.markdown(
            '<div class="info-box" style="margin-top:8px">'
            '💡 Chaque clic applique un déplacement de <code>pas</code> cm.<br>'
            'Cliquez autant de fois que souhaité — les déplacements sont cumulatifs et illimités.'
            '</div>',
            unsafe_allow_html=True,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # PIVOTER
    # ══════════════════════════════════════════════════════════════════════════
    with t_rot:
        c1, c2 = st.columns([3, 1])
        rstep = c1.number_input(
            "Pas de rotation (°)", min_value=0.1, max_value=180.0,
            value=st.session_state["rot_step"], step=0.5, format="%.1f", key="v_rot_step"
        )
        st.session_state["rot_step"] = rstep
        c2.markdown(f"<div style='padding-top:28px;font-size:10px;color:#888'>{rstep:.1f}°</div>", unsafe_allow_html=True)

        def _rot(axis, deg):
            idx = _obj_idx(obj_df, sel_oid)
            if idx is None: return
            df2 = obj_df.copy()
            nx, ny, nz, nw = compose_rot(
                float(df2.at[idx,"rot_x"]), float(df2.at[idx,"rot_y"]),
                float(df2.at[idx,"rot_z"]), float(df2.at[idx,"rot_w"]),
                axis, deg
            )
            df2.at[idx,"rot_x"] = nx; df2.at[idx,"rot_y"] = ny
            df2.at[idx,"rot_z"] = nz; df2.at[idx,"rot_w"] = nw
            save_parquet(df2, OBJ_KEY)
            st.rerun()

        st.markdown('<p class="move-lbl">Axe Y — pivot horizontal</p>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        if c1.button(f"↺  Y − {rstep:.1f}°", key="ry_m", use_container_width=True):
            _rot("y", -rstep)
        if c2.button(f"↻  Y + {rstep:.1f}°", key="ry_p", use_container_width=True):
            _rot("y", +rstep)

        st.markdown('<p class="move-lbl">Axe X — inclinaison avant / arrière</p>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        if c1.button(f"↺  X − {rstep:.1f}°", key="rx_m", use_container_width=True):
            _rot("x", -rstep)
        if c2.button(f"↻  X + {rstep:.1f}°", key="rx_p", use_container_width=True):
            _rot("x", +rstep)

        st.markdown('<p class="move-lbl">Axe Z — roulis gauche / droite</p>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        if c1.button(f"↺  Z − {rstep:.1f}°", key="rz_m", use_container_width=True):
            _rot("z", -rstep)
        if c2.button(f"↻  Z + {rstep:.1f}°", key="rz_p", use_container_width=True):
            _rot("z", +rstep)

        st.markdown(
            f'<div class="info-box" style="margin-top:8px">'
            f'Rotation actuelle : <b>X {ex:.1f}°</b> &nbsp; <b>Y {ey:.1f}°</b> &nbsp; <b>Z {ez:.1f}°</b><br>'
            f'<br>💡 Maintenez un rythme de clics rapides pour pivoter en continu.<br>'
            f'Utilisez un pas de 45° pour des angles nets, 1° pour la précision.</div>',
            unsafe_allow_html=True,
        )

        if st.button("⟲ Réinitialiser rotation", key="rot_reset"):
            idx = _obj_idx(obj_df, sel_oid)
            if idx is not None:
                df2 = obj_df.copy()
                df2.at[idx,"rot_x"]=0.0; df2.at[idx,"rot_y"]=0.0
                df2.at[idx,"rot_z"]=0.0; df2.at[idx,"rot_w"]=1.0
                save_parquet(df2, OBJ_KEY)
                st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # ÉCHELLE
    # ══════════════════════════════════════════════════════════════════════════
    with t_scl:
        st.markdown(
            f'<div class="pos-display">'
            f'<div class="pos-axis"><span>SX</span>{sx:.2f}</div>'
            f'<div class="pos-axis"><span>SY</span>{sy:.2f}</div>'
            f'<div class="pos-axis"><span>SZ</span>{sz:.2f}</div>'
            f'</div>', unsafe_allow_html=True,
        )

        sstep = st.number_input(
            "Pas d'échelle", min_value=0.01, max_value=100.0,
            value=st.session_state["scale_step"], step=0.05, format="%.2f", key="v_scale_step"
        )
        st.session_state["scale_step"] = sstep
        uniform = st.checkbox("Uniforme (X=Y=Z)", value=True, key="scl_uni")

        def _scl(ds, axis=None):
            idx = _obj_idx(obj_df, sel_oid)
            if idx is None: return
            df2 = obj_df.copy()
            axes = ["scale_x","scale_y","scale_z"] if uniform else ([f"scale_{axis}"] if axis else ["scale_x"])
            for a in axes:
                df2.at[idx,a] = max(0.01, float(df2.at[idx,a]) + ds)
            save_parquet(df2, OBJ_KEY)
            st.rerun()

        if uniform:
            c1, c2 = st.columns(2)
            if c1.button(f"▲  +{sstep:.2f} (uniforme)", key="su_p", use_container_width=True):
                _scl(+sstep)
            if c2.button(f"▼  −{sstep:.2f} (uniforme)", key="su_m", use_container_width=True):
                _scl(-sstep)
        else:
            for lbl2, ax in [("X","x"),("Y","y"),("Z","z")]:
                c1, c2 = st.columns(2)
                if c1.button(f"▲  {lbl2}+", key=f"s{ax}p", use_container_width=True):
                    _scl(+sstep, ax)
                if c2.button(f"▼  {lbl2}−", key=f"s{ax}m", use_container_width=True):
                    _scl(-sstep, ax)

        st.divider()
        st.markdown("**Scaling interne (sur les points)**")
        c1, c2 = st.columns(2)
        int_mult = c1.number_input("Multiplicateur", value=1.0, step=0.1, min_value=0.01, key="int_mult")
        int_cx   = c2.number_input("Centre X (cm)",  value=0.0, step=1.0, key="int_cx")
        if st.button("Appliquer scaling interne", key="apply_int"):
            if not pts_df.empty:
                mask = pts_df["object_id"] == sel_oid
                if mask.any():
                    pts2 = pts_df.copy()
                    pts2.loc[mask,"x"] = (pts2.loc[mask,"x"] - int_cx)*int_mult + int_cx
                    pts2.loc[mask,"y"] = pts2.loc[mask,"y"]*int_mult
                    pts2.loc[mask,"z"] = pts2.loc[mask,"z"]*int_mult
                    save_parquet(pts2, PTS_KEY)
                    st.success("Points re-scalés !")
                    st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # POSITION EXACTE
    # ══════════════════════════════════════════════════════════════════════════
    with t_abs:
        st.markdown("**Position absolue (cm)**")
        c1, c2, c3 = st.columns(3)
        npx = c1.number_input("X", value=px, step=1.0, format="%.1f", key=f"apx_{sel_oid}")
        npy = c2.number_input("Y", value=py, step=1.0, format="%.1f", key=f"apy_{sel_oid}")
        npz = c3.number_input("Z", value=pz, step=1.0, format="%.1f", key=f"apz_{sel_oid}")
        if st.button("Appliquer position", key="abs_pos"):
            idx = _obj_idx(obj_df, sel_oid)
            if idx is not None:
                df2 = obj_df.copy()
                df2.at[idx,"pos_x"]=npx; df2.at[idx,"pos_y"]=npy; df2.at[idx,"pos_z"]=npz
                save_parquet(df2, OBJ_KEY); st.rerun()

        st.markdown("**Rotation absolue (degrés)**")
        c1, c2, c3 = st.columns(3)
        nrx = c1.number_input("X°", value=round(ex,2), step=1.0, format="%.2f", key=f"arx_{sel_oid}")
        nry = c2.number_input("Y°", value=round(ey,2), step=1.0, format="%.2f", key=f"ary_{sel_oid}")
        nrz = c3.number_input("Z°", value=round(ez,2), step=1.0, format="%.2f", key=f"arz_{sel_oid}")
        if st.button("Appliquer rotation", key="abs_rot"):
            aqx, aqy, aqz, aqw = euler_to_quat(nrx, nry, nrz)
            idx = _obj_idx(obj_df, sel_oid)
            if idx is not None:
                df2 = obj_df.copy()
                df2.at[idx,"rot_x"]=aqx; df2.at[idx,"rot_y"]=aqy
                df2.at[idx,"rot_z"]=aqz; df2.at[idx,"rot_w"]=aqw
                save_parquet(df2, OBJ_KEY); st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # SUPPRIMER
    # ══════════════════════════════════════════════════════════════════════════
    with t_del:
        st.warning(f"⚠️ Supprimer **{obj['name']}** et tous ses points / segments ?")
        if st.button("🗑 Confirmer", key="del_obj_confirm"):
            df2 = obj_df[obj_df["object_id"] != sel_oid]
            p2  = pts_df[pts_df["object_id"] != sel_oid] if not pts_df.empty else pts_df
            s2  = seg_df[seg_df["object_id"]  != sel_oid] if not seg_df.empty else seg_df
            for df_, k_ in [(df2,OBJ_KEY),(p2,PTS_KEY),(s2,SEG_KEY)]:
                save_parquet(df_, k_)
            st.session_state["object_id"] = None
            st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# OBJECT DESIGNER — points & segments
# ─────────────────────────────────────────────────────────────────────────────

def panel_object_designer(obj_df, pts_df, seg_df, sel_oid):

    if sel_oid is None:
        st.markdown(
            '<div class="info-box">👆 Sélectionnez un objet pour éditer ses points.</div>',
            unsafe_allow_html=True,
        )
        return

    if obj_df[obj_df["object_id"] == sel_oid].empty:
        return

    o_pts  = pts_df[pts_df["object_id"] == sel_oid] if not pts_df.empty else pd.DataFrame()
    o_segs = seg_df[seg_df["object_id"]  == sel_oid] if not seg_df.empty else pd.DataFrame()

    tab_pts, tab_segs, tab_csv = st.tabs(["📍 Points", "🔗 Segments", "⬇ Import CSV"])

    # ══════════════════════════════════════════════════════════════════════════
    # POINTS
    # ══════════════════════════════════════════════════════════════════════════
    with tab_pts:
        with st.expander("➕ Ajouter un point", expanded=o_pts.empty):
            c1,c2,c3,c4 = st.columns([2,2,2,1])
            nx=c1.number_input("X(cm)",value=0.0,step=1.0,format="%.1f",key="np_x")
            ny=c2.number_input("Y(cm)",value=0.0,step=1.0,format="%.1f",key="np_y")
            nz=c3.number_input("Z(cm)",value=0.0,step=1.0,format="%.1f",key="np_z")
            c4.markdown("<br>", unsafe_allow_html=True)
            if c4.button("OK", key="add_pt"):
                pid = next_id(pts_df, "point_id")
                pts2 = pd.concat([pts_df, pd.DataFrame([{
                    "point_id":pid,"object_id":sel_oid,
                    "x":float(nx),"y":float(ny),"z":float(nz),
                }])], ignore_index=True)
                save_parquet(pts2, PTS_KEY); st.rerun()

        if o_pts.empty:
            st.info("Aucun point.")
            return

        # Sélection du point à manipuler
        pt_map = {
            f"#{int(r['point_id'])}  ({float(r['x']):.1f}, {float(r['y']):.1f}, {float(r['z']):.1f})": int(r["point_id"])
            for _, r in o_pts.iterrows()
        }
        sel_lbl = st.selectbox("Point actif", list(pt_map.keys()), key="sel_pt_lbl")
        sel_pid = pt_map[sel_lbl]
        pt_row  = o_pts[o_pts["point_id"] == sel_pid].iloc[0]
        cx, cy, cz = float(pt_row["x"]), float(pt_row["y"]), float(pt_row["z"])

        st.markdown(
            f'<div class="pos-display">'
            f'<div class="pos-axis"><span>X</span>{cx:.1f} cm</div>'
            f'<div class="pos-axis"><span>Y</span>{cy:.1f} cm</div>'
            f'<div class="pos-axis"><span>Z</span>{cz:.1f} cm</div>'
            f'</div>', unsafe_allow_html=True,
        )

        pstep = st.number_input(
            "Pas (cm)", min_value=0.1, max_value=9999.0,
            value=st.session_state["pt_move_step"], step=0.1, format="%.1f", key="pt_step_v"
        )
        st.session_state["pt_move_step"] = pstep

        def _mpt(dx=0.0, dy=0.0, dz=0.0):
            idx = pts_df.index[pts_df["point_id"] == sel_pid][0]
            df2 = pts_df.copy()
            df2.at[idx,"x"] += dx; df2.at[idx,"y"] += dy; df2.at[idx,"z"] += dz
            save_parquet(df2, PTS_KEY); st.rerun()

        st.markdown('<p class="move-lbl">Plan XZ</p>', unsafe_allow_html=True)
        _, tc, _ = st.columns([1,1,1])
        if tc.button("⬆ −Z", key="pt_mz", use_container_width=True):
            _mpt(dz=-pstep)
        lc2, mc2, rc2 = st.columns(3)
        if lc2.button("◀ −X", key="pt_mx", use_container_width=True):
            _mpt(dx=-pstep)
        mc2.markdown(f"<div style='text-align:center;padding:8px 0;border:1px solid #21262d;border-radius:5px;font-size:10px;color:#888'>X{cx:.1f}<br>Z{cz:.1f}</div>", unsafe_allow_html=True)
        if rc2.button("▶ +X", key="pt_px", use_container_width=True):
            _mpt(dx=+pstep)
        _, bc, _ = st.columns([1,1,1])
        if bc.button("⬇ +Z", key="pt_pz", use_container_width=True):
            _mpt(dz=+pstep)

        st.markdown('<p class="move-lbl">Vertical Y</p>', unsafe_allow_html=True)
        yc1, yc2, yc3 = st.columns(3)
        if yc1.button("▲ +Y", key="pt_py", use_container_width=True):
            _mpt(dy=+pstep)
        yc2.markdown(f"<div style='text-align:center;padding:6px 0;font-size:10px;color:#888'>Y {cy:.1f}</div>", unsafe_allow_html=True)
        if yc3.button("▼ −Y", key="pt_my", use_container_width=True):
            _mpt(dy=-pstep)

        st.divider()
        st.markdown("**Édition directe**")
        edit = st.data_editor(
            o_pts[["point_id","x","y","z"]].reset_index(drop=True),
            key=f"pts_edit_{sel_oid}", use_container_width=True, hide_index=True,
            column_config={
                "point_id": st.column_config.NumberColumn("ID", disabled=True, width="small"),
                "x": st.column_config.NumberColumn("X (cm)", step=0.1, format="%.1f"),
                "y": st.column_config.NumberColumn("Y (cm)", step=0.1, format="%.1f"),
                "z": st.column_config.NumberColumn("Z (cm)", step=0.1, format="%.1f"),
            },
        )
        c1, c2 = st.columns(2)
        if c1.button("💾 Sauvegarder", key="save_pts"):
            df2 = pts_df.copy()
            for _, r in edit.iterrows():
                if pd.notna(r.get("point_id")):
                    idx = df2.index[df2["point_id"] == int(r["point_id"])]
                    if len(idx):
                        df2.at[idx[0],"x"]=float(r["x"])
                        df2.at[idx[0],"y"]=float(r["y"])
                        df2.at[idx[0],"z"]=float(r["z"])
            save_parquet(df2, PTS_KEY); st.success("Sauvegardé"); st.rerun()

        if c2.button("🗑 Supprimer point", key="del_pt"):
            p2 = pts_df[pts_df["point_id"] != sel_pid]
            s2 = seg_df[(seg_df["point_a_id"] != sel_pid)&(seg_df["point_b_id"] != sel_pid)] \
                 if not seg_df.empty else seg_df
            save_parquet(p2, PTS_KEY); save_parquet(s2, SEG_KEY); st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # SEGMENTS
    # ══════════════════════════════════════════════════════════════════════════
    with tab_segs:
        if o_pts.empty or len(o_pts) < 2:
            st.info("Au moins 2 points requis."); return

        pt_lbl = {
            f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})": int(r["point_id"])
            for _, r in o_pts.iterrows()
        }
        lbls = list(pt_lbl.keys())
        c1, c2 = st.columns(2)
        sa = c1.selectbox("Point A", lbls, key="seg_a")
        sb = c2.selectbox("Point B", lbls, key="seg_b", index=min(1,len(lbls)-1))

        if st.button("🔗 Créer segment", key="mk_seg"):
            pa_id, pb_id = pt_lbl[sa], pt_lbl[sb]
            if pa_id == pb_id:
                st.error("Deux points distincts requis.")
            else:
                dupe = (not seg_df.empty) and not seg_df[
                    (seg_df["object_id"]==sel_oid) &
                    (((seg_df["point_a_id"]==pa_id)&(seg_df["point_b_id"]==pb_id))|
                     ((seg_df["point_a_id"]==pb_id)&(seg_df["point_b_id"]==pa_id)))
                ].empty
                if dupe:
                    st.warning("Segment déjà existant.")
                else:
                    sid = next_id(seg_df, "segment_id")
                    s2  = pd.concat([seg_df, pd.DataFrame([{
                        "segment_id":sid,"object_id":sel_oid,
                        "point_a_id":pa_id,"point_b_id":pb_id,
                    }])], ignore_index=True)
                    save_parquet(s2, SEG_KEY); st.rerun()

        if not o_segs.empty:
            st.markdown(f"**{len(o_segs)} segment(s)**")
            st.dataframe(o_segs[["segment_id","point_a_id","point_b_id"]].reset_index(drop=True),
                         use_container_width=True, hide_index=True)
            c1, c2 = st.columns([3,1])
            dsid = c1.selectbox("Supprimer", o_segs["segment_id"].tolist(), key="dseg_sel")
            if c2.button("🗑", key="dseg_btn"):
                save_parquet(seg_df[seg_df["segment_id"]!=dsid], SEG_KEY); st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # CSV
    # ══════════════════════════════════════════════════════════════════════════
    with tab_csv:
        st.markdown('<div class="info-box">Format : <code>x,y,z</code> par ligne (cm)</div>', unsafe_allow_html=True)
        up = st.file_uploader("CSV", type=["csv"], key="csv_up")
        if up:
            try:
                dfc = pd.read_csv(up, names=["x","y","z"])
                st.dataframe(dfc.head(10), use_container_width=True)
                st.markdown(f"**{len(dfc)} points**")
                if st.button("⬇ Importer", key="do_import"):
                    base = next_id(pts_df, "point_id")
                    new  = [{"point_id":base+i,"object_id":sel_oid,
                              "x":float(r["x"]),"y":float(r["y"]),"z":float(r["z"])}
                             for i,(_,r) in enumerate(dfc.iterrows())]
                    save_parquet(pd.concat([pts_df, pd.DataFrame(new)], ignore_index=True), PTS_KEY)
                    st.success(f"{len(new)} points importés !"); st.rerun()
            except Exception as exc:
                st.error(f"Erreur : {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    init_session()

    if not st.session_state["r2_ready"]:
        try:
            init_r2_tables()
            st.session_state["r2_ready"] = True
        except Exception as exc:
            st.warning(f"R2 non disponible : {exc}")

    proj_df = load_parquet(PROJ_KEY, PROJ_COLS)
    obj_df  = load_parquet(OBJ_KEY,  OBJ_COLS)
    pts_df  = load_parquet(PTS_KEY,  PTS_COLS)
    seg_df  = load_parquet(SEG_KEY,  SEG_COLS)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown(
            '<div class="studio-header">'
            '<div><div class="studio-title">🧊 3D Design Studio</div>'
            '<div class="studio-sub">Point · Segment · Transform</div></div>'
            '</div>',
            unsafe_allow_html=True,
        )

        mode_lbl = st.radio(
            "Mode", ["📐 Plan Editor", "✏️ Object Designer"],
            index=0 if st.session_state["mode"]=="plan_editor" else 1,
            horizontal=True, label_visibility="collapsed",
        )
        st.session_state["mode"] = "plan_editor" if "Plan" in mode_lbl else "object_designer"

        bcls = "badge-plan" if st.session_state["mode"]=="plan_editor" else "badge-object"
        blbl = "PLAN EDITOR" if st.session_state["mode"]=="plan_editor" else "OBJECT DESIGNER"
        st.markdown(f'<span class="badge {bcls}">{blbl}</span>', unsafe_allow_html=True)

        st.divider()

        # Projets
        st.markdown('<p class="section-label">📁 Projets</p>', unsafe_allow_html=True)
        with st.expander("Nouveau projet", expanded=proj_df.empty):
            pname = st.text_input("Nom", key="new_proj_name", placeholder="Mon projet…")
            if st.button("Créer", key="create_proj"):
                if pname.strip():
                    pid = next_id(proj_df, "project_id")
                    proj_df = pd.concat([proj_df, pd.DataFrame([{
                        "project_id":pid,"name":pname.strip(),
                        "created_at":datetime.now().isoformat(),
                    }])], ignore_index=True)
                    save_parquet(proj_df, PROJ_KEY)
                    st.session_state["project_id"]=pid
                    st.session_state["object_id"]=None
                    st.rerun()

        if not proj_df.empty:
            pnames=proj_df["name"].tolist(); pids=proj_df["project_id"].tolist()
            cur=st.session_state["project_id"]; ci=pids.index(cur) if cur in pids else 0
            sel_pn=st.selectbox("Projet",pnames,index=ci,key="proj_sel",label_visibility="collapsed")
            st.session_state["project_id"]=pids[pnames.index(sel_pn)]
            if st.button("🗑 Supprimer projet", key="del_proj"):
                dpid=st.session_state["project_id"]
                proj_df=proj_df[proj_df["project_id"]!=dpid]
                if not obj_df.empty:
                    doids=obj_df[obj_df["project_id"]==dpid]["object_id"].tolist()
                    obj_df=obj_df[obj_df["project_id"]!=dpid]
                    if not pts_df.empty: pts_df=pts_df[~pts_df["object_id"].isin(doids)]
                    if not seg_df.empty: seg_df=seg_df[~seg_df["object_id"].isin(doids)]
                for df_,k_ in [(proj_df,PROJ_KEY),(obj_df,OBJ_KEY),(pts_df,PTS_KEY),(seg_df,SEG_KEY)]:
                    save_parquet(df_,k_)
                st.session_state["project_id"]=None; st.session_state["object_id"]=None; st.rerun()
        else:
            st.caption("Aucun projet.")

        st.divider()

        # Objets
        cur_pid=st.session_state.get("project_id")
        st.markdown('<p class="section-label">📦 Objets</p>', unsafe_allow_html=True)
        if cur_pid is not None:
            with st.expander("Nouvel objet"):
                oname=st.text_input("Nom",key="new_obj_name",placeholder="Objet A…")
                if st.button("Créer",key="create_obj"):
                    oid=next_id(obj_df,"object_id")
                    obj_df=pd.concat([obj_df,pd.DataFrame([{
                        "object_id":oid,"project_id":cur_pid,
                        "name":oname.strip() or f"Objet {oid}",
                        "pos_x":0.0,"pos_y":0.0,"pos_z":0.0,
                        "rot_x":0.0,"rot_y":0.0,"rot_z":0.0,"rot_w":1.0,
                        "scale_x":1.0,"scale_y":1.0,"scale_z":1.0,
                    }])],ignore_index=True)
                    save_parquet(obj_df,OBJ_KEY); st.session_state["object_id"]=oid; st.rerun()

            proj_objs=obj_df[obj_df["project_id"]==cur_pid] if not obj_df.empty else pd.DataFrame()
            sel_oid=st.session_state.get("object_id")
            if not proj_objs.empty:
                for _,o in proj_objs.iterrows():
                    oid2=int(o["object_id"]); active=oid2==sel_oid
                    np_=len(pts_df[pts_df["object_id"]==oid2]) if not pts_df.empty else 0
                    lbl=f"{'▶ ' if active else '  '}{o['name']} · {np_}pt"
                    if st.button(lbl,key=f"sel_{oid2}",use_container_width=True):
                        st.session_state["object_id"]=oid2; st.rerun()
            else:
                st.caption("Aucun objet.")
        else:
            st.caption("Sélectionnez un projet.")

        st.divider()
        st.markdown('<p class="section-label">👁 Affichage</p>', unsafe_allow_html=True)
        c1,c2=st.columns(2)
        st.session_state["show_grid"]=c1.checkbox("Grille",value=True)
        st.session_state["show_axes"]=c2.checkbox("Axes",  value=True)
        st.session_state["snap"]=st.checkbox("Snap visuel",value=True)
        if st.session_state["snap"]:
            st.session_state["snap_dist"]=st.slider(
                "Seuil snap",0.5,30.0,5.0,0.5,label_visibility="collapsed"
            )

    # ── Zone principale ───────────────────────────────────────────────────────
    cur_oid = st.session_state.get("object_id")
    cur_pts = st.session_state.get("selected_pts", [])
    cur_pid = st.session_state.get("project_id")

    scene = build_scene_json(cur_pid, obj_df, pts_df, seg_df, cur_oid, cur_pts)
    render_viewer(scene, st.session_state["mode"], height=520)

    st.markdown("<hr style='border-color:#21262d;margin:8px 0'>", unsafe_allow_html=True)

    if st.session_state["mode"] == "plan_editor":
        panel_plan_editor(obj_df, pts_df, seg_df, cur_oid)
    else:
        panel_object_designer(obj_df, pts_df, seg_df, cur_oid)


if __name__ == "__main__":
    main()