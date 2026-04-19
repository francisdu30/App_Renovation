"""
3D Design Studio v3
Streamlit + Three.js + Cloudflare R2 (Parquet)
Nouveautés : grille éphémère, touche Suppr, surbrillance coïncidence, ancre objet
"""

import io, json, math
from datetime import datetime
import boto3
import pandas as pd
import streamlit as st
from streamlit.components.v1 import html as st_html

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="3D Design Studio", page_icon="🧊",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;700;800&display=swap');
html,body,[class*="css"]{font-family:'JetBrains Mono',monospace;}
:root{
  --bg0:#0a0c10;--bg1:#0f1117;--bg2:#161b22;--bg3:#1c2333;
  --border:#21262d;--accent:#1a73e8;--accent2:#2e7d32;--accent3:#f78166;
  --text0:#e6edf3;--text1:#8b949e;--text2:#484f58;
}
.stApp{background:var(--bg0);}
section[data-testid="stSidebar"]{background:var(--bg1)!important;border-right:1px solid var(--border);}
section[data-testid="stSidebar"]>div{padding-top:.5rem;}
.main .block-container{padding:.75rem 1rem 1rem 1rem;max-width:100%;}

.studio-header{display:flex;align-items:center;gap:10px;padding:12px 0 8px 0;border-bottom:1px solid var(--border);margin-bottom:12px;}
.studio-title{font-family:'Syne',sans-serif;font-size:18px;font-weight:800;color:var(--accent);letter-spacing:-.5px;}
.studio-sub{font-size:9px;color:var(--text2);letter-spacing:2px;text-transform:uppercase;}

.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;letter-spacing:1px;text-transform:uppercase;}
.badge-plan{background:#1a2744;color:#58a6ff;border:1px solid #1f3a72;}
.badge-object{background:#2a1a1a;color:#f78166;border:1px solid #5a2a2a;}
.section-label{font-size:9px;letter-spacing:2px;text-transform:uppercase;color:var(--text2);margin:8px 0 4px 0;}

.metric-row{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:8px 0;}
.metric-card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:8px 10px;text-align:center;}
.metric-val{font-size:16px;font-weight:700;color:var(--accent);}
.metric-lbl{font-size:9px;color:var(--text2);letter-spacing:1px;text-transform:uppercase;}

.pos-display{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:8px 12px;margin:6px 0;display:flex;gap:16px;align-items:center;flex-wrap:wrap;}
.pos-axis{font-size:11px;}
.pos-axis span{color:var(--text2);font-size:9px;text-transform:uppercase;margin-right:3px;}
.move-lbl{font-size:9px;color:var(--text2);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:3px;margin-top:8px;}

.pending-box{background:#1a2a1a;border:1px solid #2a5a2a;border-radius:6px;padding:10px 12px;font-size:11px;color:#6ab06a;margin:8px 0;}
.pending-box strong{color:#3fb950;}
.info-box{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:10px 12px;font-size:11px;color:var(--text1);margin:8px 0;}
.info-box code{background:var(--bg3);padding:1px 4px;border-radius:3px;color:#3fb950;font-size:10px;}
.viewer-wrap{border-radius:8px;overflow:hidden;border:1px solid var(--border);}

/* hide the message bus input */
div[data-testid="stTextInput"]:has(input[placeholder="__3ds__"]){
  position:absolute!important;opacity:0!important;pointer-events:none!important;
  width:1px!important;height:1px!important;overflow:hidden!important;
}

.stButton>button{background:var(--bg2)!important;border:1px solid var(--border)!important;color:var(--text0)!important;font-family:'JetBrains Mono',monospace!important;font-size:11px!important;border-radius:5px!important;transition:all .15s ease!important;}
.stButton>button:hover{border-color:var(--accent)!important;color:var(--accent)!important;}
.stTabs [data-baseweb="tab"]{font-family:'JetBrains Mono',monospace;font-size:11px;}
div[data-testid="stNumberInput"] input{font-family:'JetBrains Mono',monospace;font-size:12px;background:var(--bg2)!important;border-color:var(--border)!important;color:var(--text0)!important;}
div[data-testid="stDataFrame"]{font-size:11px;}
.stAlert{font-size:11px;}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# R2 / PARQUET
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_r2():
    return boto3.client("s3",
        endpoint_url=f"https://{st.secrets['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=st.secrets["R2_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["R2_SECRET_KEY"],
        region_name="auto")

def load_parquet(key, cols):
    try:
        obj = get_r2().get_object(Bucket=st.secrets["R2_BUCKET"], Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        return pd.DataFrame(columns=cols)

def save_parquet(df, key):
    buf = io.BytesIO(); df.to_parquet(buf, index=False, compression="zstd"); buf.seek(0)
    get_r2().put_object(Bucket=st.secrets["R2_BUCKET"], Key=key, Body=buf.getvalue())

PROJ_KEY = "projects.parquet"
OBJ_KEY  = "objects.parquet"
PTS_KEY  = "points.parquet"
SEG_KEY  = "segments.parquet"

PROJ_COLS = ["project_id","name","created_at"]
OBJ_COLS_BASE = ["object_id","project_id","name","pos_x","pos_y","pos_z",
                  "rot_x","rot_y","rot_z","rot_w","scale_x","scale_y","scale_z"]
OBJ_COLS_EXT  = {"anchor_x":0.0,"anchor_y":0.0,"anchor_z":0.0}  # new columns
PTS_COLS  = ["point_id","object_id","x","y","z"]
SEG_COLS  = ["segment_id","object_id","point_a_id","point_b_id"]

def load_objects():
    df = load_parquet(OBJ_KEY, OBJ_COLS_BASE)
    for col, default in OBJ_COLS_EXT.items():
        if col not in df.columns:
            df[col] = default
    return df

def next_id(df, col):
    if df.empty or col not in df.columns or df[col].isnull().all(): return 1
    return int(df[col].max()) + 1

def init_r2_tables():
    for key, cols in [(PROJ_KEY,PROJ_COLS),(OBJ_KEY,OBJ_COLS_BASE),(PTS_KEY,PTS_COLS),(SEG_KEY,SEG_COLS)]:
        if load_parquet(key, cols).empty:
            try: save_parquet(pd.DataFrame(columns=cols), key)
            except: pass

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────
def _ss(k, v):
    if k not in st.session_state: st.session_state[k] = v

def init_session():
    _ss("mode","plan_editor"); _ss("project_id",None); _ss("object_id",None)
    _ss("selected_pts",[]); _ss("show_grid",True); _ss("show_axes",True)
    _ss("snap",True); _ss("snap_dist",5.0); _ss("r2_ready",False)
    _ss("move_step",1.0); _ss("rot_step",5.0); _ss("scale_step",0.1); _ss("pt_move_step",1.0)
    # Grid system
    _ss("grid_cell_size",10.0)   # cm
    _ss("grid_extent",8)          # cells each side
    _ss("grid_origin",None)       # {x,y,z} cm  – persists across reruns
    _ss("grid_angle",0.0)         # degrees      – persists across reruns
    # Pending actions from viewer
    _ss("pending_pt",None)        # {x,y,z} cm
    _ss("pending_place",None)     # {x,y,z} cm  – for object placement in PE
    # Align mode
    _ss("align_mode",False)
    _ss("align_ref_pt",None)      # {local_x,local_y,local_z}
    _ss("align_ref_oid",None)

# ─────────────────────────────────────────────────────────────────────────────
# MATH HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def quat_to_euler(qx,qy,qz,qw):
    sinr=2*(qw*qx+qy*qz); cosr=1-2*(qx*qx+qy*qy)
    ex=math.degrees(math.atan2(sinr,cosr))
    sinp=2*(qw*qy-qz*qx)
    ey=math.degrees(math.asin(max(-1,min(1,sinp))))
    siny=2*(qw*qz+qx*qy); cosy=1-2*(qy*qy+qz*qz)
    ez=math.degrees(math.atan2(siny,cosy))
    return ex,ey,ez

def euler_to_quat(ex,ey,ez):
    rx,ry,rz=math.radians(ex),math.radians(ey),math.radians(ez)
    cy,sy=math.cos(rz/2),math.sin(rz/2)
    cp,sp=math.cos(ry/2),math.sin(ry/2)
    cr,sr=math.cos(rx/2),math.sin(rx/2)
    return (sr*cp*cy-cr*sp*sy, cr*sp*cy+sr*cp*sy, cr*cp*sy-sr*sp*cy, cr*cp*cy+sr*sp*sy)

def compose_rot(qx,qy,qz,qw,axis,deg):
    a=math.radians(deg)/2; c,s=math.cos(a),math.sin(a)
    dq={"x":(s,0,0,c),"y":(0,s,0,c),"z":(0,0,s,c)}[axis]
    dx,dy,dz,dw=dq
    return (dw*qx+dx*qw+dy*qz-dz*qy, dw*qy-dx*qz+dy*qw+dz*qx,
            dw*qz+dx*qy-dy*qx+dz*qw, dw*qw-dx*qx-dy*qy-dz*qz)

# ─────────────────────────────────────────────────────────────────────────────
# COINCIDENT POINT DETECTION
# ─────────────────────────────────────────────────────────────────────────────
def find_coincident_points(obj_df, pts_df, threshold_cm=0.5):
    """Returns set of point_ids that are at the same world position as a point in another object."""
    if pts_df.empty or obj_df.empty or len(pts_df) < 2:
        return set()
    world=[]
    for _,pt in pts_df.iterrows():
        oid=int(pt["object_id"])
        rows=obj_df[obj_df["object_id"]==oid]
        if rows.empty: continue
        o=rows.iloc[0]
        wx=float(pt["x"])+float(o["pos_x"])
        wy=float(pt["y"])+float(o["pos_y"])
        wz=float(pt["z"])+float(o["pos_z"])
        world.append((int(pt["point_id"]),oid,wx,wy,wz))
    coincident=set(); t2=threshold_cm**2
    for i in range(len(world)):
        for j in range(i+1,len(world)):
            if world[i][1]==world[j][1]: continue
            dx=world[i][2]-world[j][2]; dy=world[i][3]-world[j][3]; dz=world[i][4]-world[j][4]
            if dx*dx+dy*dy+dz*dz<t2:
                coincident.add(world[i][0]); coincident.add(world[j][0])
    return coincident

# ─────────────────────────────────────────────────────────────────────────────
# SCENE JSON
# ─────────────────────────────────────────────────────────────────────────────
def build_scene_json(project_id, obj_df, pts_df, seg_df, sel_obj, sel_pts, coincident_ids):
    go=st.session_state.get("grid_origin")
    scene={
        "objects":[], "showGrid":st.session_state["show_grid"],
        "showAxes":st.session_state["show_axes"], "snap":st.session_state["snap"],
        "snapDist":st.session_state["snap_dist"], "unitScale":0.01,
        "mode":st.session_state["mode"],
        "gridCellSize":st.session_state["grid_cell_size"],
        "gridExtent":st.session_state["grid_extent"],
        "gridOrigin":go,
        "gridAngle":st.session_state["grid_angle"],
        "coincident":list(coincident_ids),
        "alignMode":st.session_state["align_mode"],
    }
    if project_id is None or obj_df.empty: return scene
    for _,obj in obj_df[obj_df["project_id"]==project_id].iterrows():
        oid=int(obj["object_id"])
        o_pts=pts_df[pts_df["object_id"]==oid] if not pts_df.empty else pd.DataFrame()
        o_seg=seg_df[seg_df["object_id"]==oid]  if not seg_df.empty else pd.DataFrame()
        pts=[{"id":int(p["point_id"]),"x":float(p["x"]),"y":float(p["y"]),"z":float(p["z"]),
              "sel":int(p["point_id"]) in sel_pts,
              "coin":int(p["point_id"]) in coincident_ids}
             for _,p in o_pts.iterrows()]
        segs=[{"id":int(s["segment_id"]),"a":int(s["point_a_id"]),"b":int(s["point_b_id"])}
              for _,s in o_seg.iterrows()]
        scene["objects"].append({
            "id":oid,"name":str(obj["name"]),
            "pos":{"x":float(obj["pos_x"]),"y":float(obj["pos_y"]),"z":float(obj["pos_z"])},
            "rot":{"x":float(obj["rot_x"]),"y":float(obj["rot_y"]),"z":float(obj["rot_z"]),"w":float(obj["rot_w"])},
            "scl":{"x":float(obj["scale_x"]),"y":float(obj["scale_y"]),"z":float(obj["scale_z"])},
            "anchor":{"x":float(obj.get("anchor_x",0)),"y":float(obj.get("anchor_y",0)),"z":float(obj.get("anchor_z",0))},
            "points":pts,"segments":segs,"sel":oid==sel_obj,
        })
    return scene

# ─────────────────────────────────────────────────────────────────────────────
# ACTION PROCESSOR (viewer → Python)
# ─────────────────────────────────────────────────────────────────────────────
def process_viewer_action(raw, obj_df, pts_df, seg_df):
    """Handle actions sent from the Three.js viewer via DOM hack."""
    try:
        action=json.loads(raw)
    except Exception:
        return
    t=action.get("type","")

    # Restore grid state always
    if "gridOriginX" in action:
        st.session_state["grid_origin"]={"x":action["gridOriginX"],"y":action["gridOriginY"],"z":action["gridOriginZ"]}
    if "gridAngle" in action:
        st.session_state["grid_angle"]=float(action["gridAngle"])

    if t=="grid_click_od":
        # Object Designer: create a point at the clicked grid node
        st.session_state["pending_pt"]={"x":action["x"],"y":action["y"],"z":action["z"]}

    elif t=="grid_click_pe":
        # Plan Editor: store pending placement position
        st.session_state["pending_place"]={"x":action["x"],"y":action["y"],"z":action["z"]}

    elif t=="grid_activate":
        # Grid was activated at a new origin
        st.session_state["grid_origin"]={"x":action["x"],"y":action["y"],"z":action["z"]}
        st.session_state["grid_angle"]=float(action.get("angle",0.0))

    elif t=="grid_angle_change":
        st.session_state["grid_angle"]=float(action["angle"])

    elif t=="grid_dismiss":
        st.session_state["grid_origin"]=None

    elif t=="delete_point":
        pid=int(action["id"])
        p2=pts_df[pts_df["point_id"]!=pid]
        s2=seg_df[(seg_df["point_a_id"]!=pid)&(seg_df["point_b_id"]!=pid)] if not seg_df.empty else seg_df
        save_parquet(p2, PTS_KEY); save_parquet(s2, SEG_KEY)
        st.session_state["_viewer_msg"]=""; st.rerun()

    elif t=="delete_segment":
        sid=int(action["id"])
        save_parquet(seg_df[seg_df["segment_id"]!=sid], SEG_KEY)
        st.session_state["_viewer_msg"]=""; st.rerun()

    elif t=="select_object":
        st.session_state["object_id"]=int(action["id"])
        st.session_state["_viewer_msg"]=""; st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# THREE.JS VIEWER
# ─────────────────────────────────────────────────────────────────────────────
def render_viewer(scene, mode, height=530):
    sj=json.dumps(scene)
    is_plan=(mode=="plan_editor")
    bcls="badge-plan" if is_plan else "badge-object"
    blbl="PLAN EDITOR" if is_plan else "OBJECT DESIGNER"

    html=f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:#fff;overflow:hidden;font-family:'JetBrains Mono',monospace;}}
#wrap{{width:100%;height:{height}px;position:relative;}}
.hud{{position:absolute;pointer-events:none;font-size:10px;letter-spacing:.3px;}}
#badge{{top:10px;left:10px;padding:4px 10px;border-radius:4px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;pointer-events:none;}}
.badge-plan{{background:rgba(26,39,68,.9);color:#58a6ff;border:1px solid #1f3a72;}}
.badge-object{{background:rgba(42,26,26,.9);color:#f78166;border:1px solid #5a2a2a;}}
#coords{{bottom:10px;left:10px;color:#333;background:rgba(255,255,255,.92);padding:5px 10px;border-radius:4px;border:1px solid #ccc;font-size:11px;}}
#status{{bottom:10px;right:10px;color:#444;background:rgba(255,255,255,.92);padding:5px 10px;border-radius:4px;border:1px solid #ccc;}}
#help{{top:10px;right:10px;color:#555;background:rgba(255,255,255,.92);padding:8px 12px;border-radius:6px;border:1px solid #ccc;line-height:1.9;}}
/* Grid HUD */
#grid-hud{{
  top:56px;left:10px;
  background:rgba(10,20,40,.88);color:#58a6ff;
  border:1px solid #1f3a72;border-radius:6px;padding:8px 10px;
  display:none;pointer-events:auto;min-width:170px;
}}
#grid-hud .gh-title{{font-size:9px;letter-spacing:1.5px;text-transform:uppercase;color:#8b949e;margin-bottom:4px;}}
#grid-hud .gh-row{{font-size:11px;margin:2px 0;}}
#grid-hud .gh-dist{{color:#3fb950;margin-top:4px;font-size:11px;}}
/* Angle dial */
#angle-dial{{
  bottom:56px;left:10px;width:72px;height:72px;
  cursor:grab;display:none;pointer-events:auto;
}}
#angle-dial:active{{cursor:grabbing;}}
/* Pending dot in viewer */
#pending-dot{{
  top:50%;left:50%;transform:translate(-50%,-50%);
  width:14px;height:14px;border-radius:50%;
  background:#3fb950;border:2px solid #fff;
  display:none;animation:pulse 1s infinite;pointer-events:none;
}}
@keyframes pulse{{0%,100%{{opacity:1;transform:translate(-50%,-50%) scale(1);}}50%{{opacity:.5;transform:translate(-50%,-50%) scale(1.4);}}}}
/* Kbd hint */
#kbd-hint{{
  bottom:44px;right:10px;color:#777;background:rgba(255,255,255,.85);
  padding:5px 8px;border-radius:4px;border:1px solid #ddd;font-size:9px;line-height:1.8;
}}
</style>
</head>
<body>
<div id="wrap">
  <div id="badge" class="hud {bcls}">{blbl}</div>
  <div id="help" class="hud">
    🖱 Rotation : clic droit + glisser<br>
    🖱 Pan : molette + glisser<br>
    🖱 Zoom : molette<br>
    🖱 Clic gauche : sélectionner
  </div>
  <div id="grid-hud" class="hud">
    <div class="gh-title">⊞ Grille éphémère</div>
    <div class="gh-row" id="gh-origin">Origine : —</div>
    <div class="gh-row" id="gh-angle">Angle : 0.0°</div>
    <div class="gh-row" id="gh-cell">Pas : — cm</div>
    <div class="gh-dist" id="gh-dist">Survoler un nœud…</div>
    <div style="font-size:9px;color:#484f58;margin-top:4px">🔴 Échap → fermer · 🟡 Dial → angle</div>
  </div>
  <div id="angle-dial" class="hud">
    <svg width="72" height="72" id="dial-svg">
      <circle cx="36" cy="36" r="32" fill="rgba(10,20,40,.85)" stroke="#1f3a72" stroke-width="1.5"/>
      <line id="dial-line" x1="36" y1="36" x2="36" y2="8" stroke="#f59e0b" stroke-width="2.5" stroke-linecap="round"/>
      <circle id="dial-handle" cx="36" cy="8" r="6" fill="#f59e0b" stroke="#fff" stroke-width="1.5"/>
      <text x="36" y="40" text-anchor="middle" fill="#8b949e" font-size="9" id="dial-text">0°</text>
    </svg>
  </div>
  <div id="coords" class="hud">X: 0.0 · Y: 0.0 · Z: 0.0 cm</div>
  <div id="status" class="hud">Prêt</div>
  <div id="kbd-hint" class="hud">
    <b>Suppr</b> : suppr. sélection (OD)<br>
    <b>Clic point / sol</b> : grille éphémère<br>
    <b>Échap</b> : fermer grille
  </div>
  <div id="pending-dot" class="hud"></div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
// ════════════════════════════════════════════════════════════════════
//  DATA FROM PYTHON
// ════════════════════════════════════════════════════════════════════
const SCENE = {sj};
const MODE  = {json.dumps(mode)};
const US    = 0.01; // cm → m

// ════════════════════════════════════════════════════════════════════
//  COMMUNICATION : viewer → Streamlit (DOM hack, same-origin)
// ════════════════════════════════════════════════════════════════════
function sendAction(payload) {{
  const data = JSON.stringify(payload);
  const targets = [];
  try {{ targets.push(window.parent); }} catch(e) {{}}
  try {{ targets.push(window.parent.parent); }} catch(e) {{}}
  for (const w of targets) {{
    try {{
      const inp = w.document.querySelector('input[placeholder="__3ds__"]');
      if (inp) {{
        const setter = Object.getOwnPropertyDescriptor(w.HTMLInputElement.prototype,'value').set;
        setter.call(inp, data);
        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
        return true;
      }}
    }} catch(e) {{}}
  }}
  return false;
}}

// ════════════════════════════════════════════════════════════════════
//  RENDERER / CAMERA
// ════════════════════════════════════════════════════════════════════
const wrap = document.getElementById('wrap');
const W = wrap.clientWidth, H = {height};
const renderer = new THREE.WebGLRenderer({{antialias:true}});
renderer.setSize(W, H);
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setClearColor(0xffffff, 1);
wrap.appendChild(renderer.domElement);
const scene = new THREE.Scene();
scene.background = new THREE.Color(0xffffff);
const camera = new THREE.PerspectiveCamera(55, W/H, 0.01, 5000);
camera.position.set(8, 6, 12);
camera.lookAt(0,0,0);

// ── Lights ───────────────────────────────────────────────────────────
scene.add(new THREE.AmbientLight(0xffffff, 1.0));
const dl = new THREE.DirectionalLight(0xffffff, 0.3);
dl.position.set(10,20,10); scene.add(dl);

// ── Orbit controls ───────────────────────────────────────────────────
let sph={{theta:0.6,phi:0.9,r:18}}, tgt=new THREE.Vector3();
let isRD=false, isMD=false, lm={{x:0,y:0}};
function applyCamera(){{
  const sp=Math.sin(sph.phi),cp=Math.cos(sph.phi),st=Math.sin(sph.theta),ct=Math.cos(sph.theta);
  camera.position.set(tgt.x+sph.r*sp*st, tgt.y+sph.r*cp, tgt.z+sph.r*sp*ct);
  camera.lookAt(tgt);
}} applyCamera();
const cv=renderer.domElement;
cv.addEventListener('contextmenu',e=>e.preventDefault());
cv.addEventListener('mousedown',e=>{{
  if(e.button===2)isRD=true; if(e.button===1){{isMD=true;e.preventDefault();}}
  lm={{x:e.clientX,y:e.clientY}};
}});
window.addEventListener('mouseup',()=>{{isRD=false;isMD=false;}});
window.addEventListener('mousemove',e=>{{
  const dx=e.clientX-lm.x,dy=e.clientY-lm.y;
  lm={{x:e.clientX,y:e.clientY}};
  if(isRD){{ sph.theta-=dx*0.005; sph.phi=Math.max(0.05,Math.min(Math.PI-0.05,sph.phi+dy*0.005)); applyCamera(); }}
  if(isMD){{
    const sp=sph.r*0.0008, right=new THREE.Vector3();
    right.crossVectors(camera.getWorldDirection(new THREE.Vector3()),camera.up).normalize();
    tgt.addScaledVector(right,-dx*sp); tgt.addScaledVector(camera.up,dy*sp); applyCamera();
  }}
  updateCoords(e);
}});
cv.addEventListener('wheel',e=>{{
  e.preventDefault();
  sph.r=Math.max(0.3,Math.min(800,sph.r*(1+e.deltaY*0.001))); applyCamera();
}},{{passive:false}});

// ── Grid (background) ────────────────────────────────────────────────
if(SCENE.showGrid){{
  const g1=new THREE.GridHelper(200,200,0xe0e0e0,0xe0e0e0);
  g1.material.transparent=true; g1.material.opacity=0.6; scene.add(g1);
  const g2=new THREE.GridHelper(200,20,0xbbbbbb,0xbbbbbb); scene.add(g2);
}}
if(SCENE.showAxes){{
  const L=3, mat=new THREE.LineBasicMaterial({{color:0x999999}});
  [[[0,0,0],[L,0,0]],[[0,0,0],[0,L,0]],[[0,0,0],[0,0,L]]].forEach(pts=>
    scene.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts.map(p=>new THREE.Vector3(...p))),mat)));
  const tg=new THREE.SphereGeometry(0.04,6,4), tm=new THREE.MeshBasicMaterial({{color:0xaaaaaa}});
  [[L,0,0],[0,L,0],[0,0,L]].forEach(p=>{{ const m=new THREE.Mesh(tg,tm); m.position.set(...p); scene.add(m); }});
}}

// ════════════════════════════════════════════════════════════════════
//  EPHEMERAL GRID SYSTEM
// ════════════════════════════════════════════════════════════════════
const EPHGRID = {{
  active: false,
  origin: new THREE.Vector3(),
  angle: SCENE.gridAngle || 0,
  cellSize: SCENE.gridCellSize || 10,
  extent: SCENE.gridExtent || 8,
  group: new THREE.Group(),
  nodes: [],   // {{mesh, worldPos, ix, iz, dist}}
  hovered: null,
}};
scene.add(EPHGRID.group);

function buildEphGrid() {{
  // Clear
  while(EPHGRID.group.children.length) EPHGRID.group.remove(EPHGRID.group.children[0]);
  EPHGRID.nodes = [];
  if(!EPHGRID.active) return;

  const N=EPHGRID.extent, S=EPHGRID.cellSize*US;
  const a=EPHGRID.angle*Math.PI/180;
  const axX=new THREE.Vector3(Math.cos(a),0,Math.sin(a));
  const axZ=new THREE.Vector3(-Math.sin(a),0,Math.cos(a));

  // Lines
  const lineMat=new THREE.LineBasicMaterial({{color:0x4a90e2,transparent:true,opacity:0.45}});
  for(let i=-N;i<=N;i++){{
    const s1=EPHGRID.origin.clone().addScaledVector(axX,i*S).addScaledVector(axZ,-N*S);
    const e1=EPHGRID.origin.clone().addScaledVector(axX,i*S).addScaledVector(axZ, N*S);
    const s2=EPHGRID.origin.clone().addScaledVector(axZ,i*S).addScaledVector(axX,-N*S);
    const e2=EPHGRID.origin.clone().addScaledVector(axZ,i*S).addScaledVector(axX, N*S);
    EPHGRID.group.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([s1,e1]),lineMat));
    EPHGRID.group.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([s2,e2]),lineMat));
  }}

  // Origin cross-hair
  const originMat=new THREE.MeshBasicMaterial({{color:0xf59e0b}});
  const originGeo=new THREE.SphereGeometry(0.07,10,8);
  const originSph=new THREE.Mesh(originGeo,originMat);
  originSph.position.copy(EPHGRID.origin); EPHGRID.group.add(originSph);

  // Node spheres
  const nGeo=new THREE.SphereGeometry(0.045,8,6);
  for(let i=-N;i<=N;i++){{
    for(let j=-N;j<=N;j++){{
      const pos=EPHGRID.origin.clone().addScaledVector(axX,i*S).addScaledVector(axZ,j*S);
      const isO=(i===0&&j===0);
      const mat=new THREE.MeshBasicMaterial({{color:isO?0xf59e0b:0x4a90e2,transparent:true,opacity:isO?1:0.5}});
      const m=new THREE.Mesh(nGeo,mat);
      m.position.copy(pos);
      m.userData={{type:'gridNode',ix:i,iz:j,worldPos:pos.clone(),
                   distCm:Math.sqrt((i*EPHGRID.cellSize)**2+(j*EPHGRID.cellSize)**2)}};
      EPHGRID.group.add(m);
      EPHGRID.nodes.push(m);
    }}
  }}
}}

// Restore grid from scene state
if(SCENE.gridOrigin){{
  EPHGRID.active=true;
  EPHGRID.origin.set(SCENE.gridOrigin.x*US, SCENE.gridOrigin.y*US, SCENE.gridOrigin.z*US);
  EPHGRID.angle=SCENE.gridAngle||0;
  EPHGRID.cellSize=SCENE.gridCellSize;
  buildEphGrid();
  document.getElementById('grid-hud').style.display='block';
  document.getElementById('angle-dial').style.display='block';
}}

function activateGrid(worldOrigin){{
  EPHGRID.origin.copy(worldOrigin);
  EPHGRID.active=true;
  EPHGRID.cellSize=SCENE.gridCellSize;
  EPHGRID.extent=SCENE.gridExtent;
  buildEphGrid();
  document.getElementById('grid-hud').style.display='block';
  document.getElementById('angle-dial').style.display='block';
  updateGridHud();
  sendAction({{type:'grid_activate',
    x:EPHGRID.origin.x/US, y:EPHGRID.origin.y/US, z:EPHGRID.origin.z/US,
    angle:EPHGRID.angle}});
}}

function dismissGrid(){{
  EPHGRID.active=false; buildEphGrid();
  document.getElementById('grid-hud').style.display='none';
  document.getElementById('angle-dial').style.display='none';
  EPHGRID.hovered=null;
  sendAction({{type:'grid_dismiss'}});
}}

function updateGridHud(){{
  const o=EPHGRID.origin;
  document.getElementById('gh-origin').textContent=
    `Origine : X${{(o.x/US).toFixed(1)}} Z${{(o.z/US).toFixed(1)}}`;
  document.getElementById('gh-angle').textContent=`Angle : ${{EPHGRID.angle.toFixed(1)}}°`;
  document.getElementById('gh-cell').textContent=`Pas : ${{EPHGRID.cellSize}} cm`;
}}

// ── Angle dial drag ───────────────────────────────────────────────────
const dialEl=document.getElementById('angle-dial');
const dialSvg=document.getElementById('dial-svg');
let isDialDrag=false; let dialRect;

dialEl.addEventListener('mousedown',e=>{{
  e.stopPropagation(); e.preventDefault();
  isDialDrag=true;
  dialRect=dialEl.getBoundingClientRect();
}});
window.addEventListener('mousemove',e=>{{
  if(!isDialDrag) return;
  const cx=dialRect.left+36, cy=dialRect.top+36;
  const dx=e.clientX-cx, dy=e.clientY-cy;
  let angle=Math.atan2(dx,-dy)*180/Math.PI;
  angle=((angle%360)+360)%360;
  EPHGRID.angle=angle;
  buildEphGrid();
  // Update dial needle
  const rad=angle*Math.PI/180;
  const ex=36+28*Math.sin(rad), ey=36-28*Math.cos(rad);
  document.getElementById('dial-line').setAttribute('x2',ex);
  document.getElementById('dial-line').setAttribute('y2',ey);
  document.getElementById('dial-handle').setAttribute('cx',ex);
  document.getElementById('dial-handle').setAttribute('cy',ey);
  document.getElementById('dial-text').textContent=Math.round(angle)+'°';
  updateGridHud();
}});
window.addEventListener('mouseup',e=>{{
  if(isDialDrag){{
    isDialDrag=false;
    sendAction({{type:'grid_angle_change',angle:EPHGRID.angle,
      gridOriginX:EPHGRID.origin.x/US,gridOriginY:EPHGRID.origin.y/US,gridOriginZ:EPHGRID.origin.z/US}});
  }}
}});

// ════════════════════════════════════════════════════════════════════
//  MATERIALS
// ════════════════════════════════════════════════════════════════════
const MAT={{
  pt:     new THREE.MeshPhongMaterial({{color:0x111111,shininess:10}}),
  ptSel:  new THREE.MeshPhongMaterial({{color:0xf59e0b,shininess:60,emissive:0x3d2900}}),
  ptCoin: new THREE.MeshPhongMaterial({{color:0xff4444,shininess:80,emissive:0x330000}}),
  seg:    new THREE.LineBasicMaterial({{color:0x555555}}),
  segSel: new THREE.LineBasicMaterial({{color:0x1a73e8}}),
  snap:   new THREE.MeshBasicMaterial({{color:0x1a73e8,transparent:true,opacity:0.8}}),
}};
const ptGeo=new THREE.SphereGeometry(0.06,10,8);

// ════════════════════════════════════════════════════════════════════
//  VIEWER SELECTION STATE (JS-internal)
// ════════════════════════════════════════════════════════════════════
let vSel={{type:null,id:null,oid:null}};

// ════════════════════════════════════════════════════════════════════
//  BUILD OBJECT SCENE
// ════════════════════════════════════════════════════════════════════
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

    // Points (OD only)
    if(MODE==='object_designer'){{
      obj.points.forEach(p=>{{
        let mat;
        if(p.coin) mat=MAT.ptCoin.clone();
        else if(p.sel||(vSel.type==='point'&&vSel.id===p.id)) mat=MAT.ptSel.clone();
        else mat=MAT.pt.clone();
        const m=new THREE.Mesh(ptGeo,mat);
        m.position.set(p.x*US,p.y*US,p.z*US);
        m.userData={{type:'point',id:p.id,oid:obj.id}};
        g.add(m);
      }});
    }}

    // Segments
    obj.segments.forEach(s=>{{
      const pa=ptMap[s.a],pb=ptMap[s.b]; if(!pa||!pb) return;
      const isSel=(vSel.type==='segment'&&vSel.id===s.id);
      const mat=(isSel||obj.sel)?MAT.segSel.clone():MAT.seg.clone();
      const line=new THREE.Line(
        new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(pa.x*US,pa.y*US,pa.z*US),
          new THREE.Vector3(pb.x*US,pb.y*US,pb.z*US),
        ]),mat);
      line.userData={{type:'segment',id:s.id,oid:obj.id}};
      g.add(line);
    }});

    // Coincident points in PE mode – highlight as world-space spheres
    if(MODE==='plan_editor'){{
      obj.points.forEach(p=>{{
        if(p.coin){{
          const m=new THREE.Mesh(new THREE.SphereGeometry(0.09,10,8),
            new THREE.MeshPhongMaterial({{color:0xff4444,transparent:true,opacity:0.7,emissive:0x220000}}));
          m.position.set(p.x*US,p.y*US,p.z*US);
          g.add(m);
        }}
      }});
    }}

    // Selection bbox
    if(obj.sel&&obj.points.length>0){{
      const bb=new THREE.Box3();
      obj.points.forEach(p=>bb.expandByPoint(new THREE.Vector3(p.x*US,p.y*US,p.z*US)));
      if(!bb.isEmpty()){{ bb.min.subScalar(0.1); bb.max.addScalar(0.1); g.add(new THREE.Box3Helper(bb,0x1a73e8)); }}
    }}

    // Proxy invisible (PE picking)
    if(MODE==='plan_editor'){{
      let bb=new THREE.Box3();
      if(obj.points.length>0) obj.points.forEach(p=>bb.expandByPoint(new THREE.Vector3(p.x*US,p.y*US,p.z*US)));
      else bb.set(new THREE.Vector3(-.3,-.3,-.3),new THREE.Vector3(.3,.3,.3));
      bb.min.subScalar(0.2); bb.max.addScalar(0.2);
      const sz=new THREE.Vector3(),ct=new THREE.Vector3(); bb.getSize(sz); bb.getCenter(ct);
      const proxy=new THREE.Mesh(new THREE.BoxGeometry(sz.x,sz.y,sz.z),
        new THREE.MeshBasicMaterial({{visible:false,side:THREE.DoubleSide}}));
      proxy.position.copy(ct); proxy.userData={{type:'object',id:obj.id,name:obj.name}}; g.add(proxy);
    }}

    // Anchor sphere (PE – when editing anchor)
    if(MODE==='plan_editor'&&obj.sel){{
      const an=obj.anchor;
      const m=new THREE.Mesh(new THREE.SphereGeometry(0.08,10,8),
        new THREE.MeshBasicMaterial({{color:0x00ff88}}));
      m.position.set(an.x*US,an.y*US,an.z*US); m.userData={{type:'anchor',oid:obj.id}}; g.add(m);
    }}

    objGroups[obj.id]=g; scene.add(g);
  }});
}}
buildScene(SCENE);

// ── Snap (OD) ────────────────────────────────────────────────────────
let snapSphere=null;
if(SCENE.snap&&MODE==='object_designer'){{
  snapSphere=new THREE.Mesh(new THREE.SphereGeometry(0.09,10,8),MAT.snap.clone());
  snapSphere.visible=false; scene.add(snapSphere);
  const allPts=[];
  SCENE.objects.forEach(o=>o.points.forEach(p=>{{
    allPts.push({{w:new THREE.Vector3((o.pos.x+p.x)*US,(o.pos.y+p.y)*US,(o.pos.z+p.z)*US)}});
  }}));
  const gp=new THREE.Plane(new THREE.Vector3(0,1,0),0);
  window.addEventListener('mousemove',ev=>{{
    if(snapSphere===null) return;
    const r=cv.getBoundingClientRect();
    const m2=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
    const ray=new THREE.Raycaster(); ray.setFromCamera(m2,camera);
    const h=new THREE.Vector3(); ray.ray.intersectPlane(gp,h);
    let near=null,minD=SCENE.snapDist*US;
    allPts.forEach(ap=>{{ const d=h.distanceTo(ap.w); if(d<minD){{minD=d;near=ap;}} }});
    if(near){{ snapSphere.position.copy(near.w); snapSphere.visible=true; }}
    else snapSphere.visible=false;
  }});
}}

// ── Coordinates HUD ───────────────────────────────────────────────────
const gndPl=new THREE.Plane(new THREE.Vector3(0,1,0),0);
const coordDiv=document.getElementById('coords');
const statusDiv=document.getElementById('status');

function updateCoords(ev){{
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
  const ray=new THREE.Raycaster(); ray.setFromCamera(m,camera);
  const h=new THREE.Vector3();
  if(ray.ray.intersectPlane(gndPl,h))
    coordDiv.textContent=`X:${{(h.x/US).toFixed(1)}} · Y:${{(h.y/US).toFixed(1)}} · Z:${{(h.z/US).toFixed(1)}} cm`;
}}

// ── Grid node hover ───────────────────────────────────────────────────
const pickRay=new THREE.Raycaster();
pickRay.params.Line={{threshold:0.06}};

let lastMouseEv=null;
window.addEventListener('mousemove',ev=>{{
  lastMouseEv=ev;
  if(!EPHGRID.active) return;
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
  pickRay.setFromCamera(m,camera);
  const hits=pickRay.intersectObjects(EPHGRID.nodes,false);
  if(hits.length>0){{
    const n=hits[0].object;
    if(EPHGRID.hovered!==n){{
      if(EPHGRID.hovered) EPHGRID.hovered.material.color.set(0x4a90e2);
      EPHGRID.hovered=n;
      n.material.color.set(0x3fb950);
      n.material.opacity=1;
    }}
    document.getElementById('gh-dist').textContent=
      `Distance: ${{n.userData.distCm.toFixed(1)}} cm`;
  }} else {{
    if(EPHGRID.hovered){{ EPHGRID.hovered.material.color.set(0x4a90e2); EPHGRID.hovered.material.opacity=0.5; EPHGRID.hovered=null; }}
    document.getElementById('gh-dist').textContent='Survoler un nœud…';
  }}
}});

// ════════════════════════════════════════════════════════════════════
//  CLICK HANDLER
// ════════════════════════════════════════════════════════════════════
cv.addEventListener('click',ev=>{{
  if(isRD) return;
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
  pickRay.setFromCamera(m,camera);

  // 1) Check grid nodes first (when grid is active)
  if(EPHGRID.active){{
    const gHits=pickRay.intersectObjects(EPHGRID.nodes,false);
    if(gHits.length>0){{
      const node=gHits[0].object;
      const wp=node.userData.worldPos;
      const payload={{
        x:wp.x/US, y:wp.y/US, z:wp.z/US,
        gridOriginX:EPHGRID.origin.x/US,gridOriginY:EPHGRID.origin.y/US,gridOriginZ:EPHGRID.origin.z/US,
        gridAngle:EPHGRID.angle,
        type: MODE==='object_designer'?'grid_click_od':'grid_click_pe',
      }};
      sendAction(payload);
      statusDiv.textContent=`Nœud sélectionné : ${{node.userData.distCm.toFixed(1)}} cm (voir panneau)`;
      // Pulse pending-dot near the node for feedback
      const dot=document.getElementById('pending-dot');
      dot.style.display='block'; setTimeout(()=>{{dot.style.display='none';}},2000);
      return;
    }}
  }}

  // 2) Pick objects / points
  const tgts=[];
  Object.values(objGroups).forEach(g=>g.traverse(c=>{{ if(c.userData&&c.userData.type) tgts.push(c); }}));
  const hits=pickRay.intersectObjects(tgts,false);

  if(hits.length>0){{
    const ud=hits[0].object.userData;
    if(MODE==='plan_editor'){{
      // Always select object
      const oid=ud.oid||ud.id;
      vSel={{type:'object',id:oid,oid}};
      sendAction({{type:'select_object',id:oid}});
      statusDiv.textContent='Objet #'+oid+' sélectionné';
    }} else {{
      // OD mode: select point or segment
      vSel={{type:ud.type,id:ud.id,oid:ud.oid}};
      buildScene(SCENE); // redraw with new selection
      statusDiv.textContent=(ud.type==='point'?'Point':'Segment')+' #'+ud.id+' — Suppr pour supprimer';
    }}
    return;
  }}

  // 3) Click on ground → activate grid
  const gHit=new THREE.Vector3();
  if(pickRay.ray.intersectPlane(gndPl,gHit)){{
    activateGrid(gHit);
    statusDiv.textContent='Grille activée — cliquez un nœud pour créer/placer';
  }}
}});

// ════════════════════════════════════════════════════════════════════
//  KEYBOARD HANDLER
// ════════════════════════════════════════════════════════════════════
cv.addEventListener('keydown',ev=>ev.stopPropagation()); // prevent Streamlit catching keys
window.addEventListener('keydown',ev=>{{
  // Delete selected item (OD mode)
  if((ev.key==='Delete'||ev.key==='Backspace')&&MODE==='object_designer'){{
    if(vSel.type==='point'){{
      sendAction({{type:'delete_point',id:vSel.id,
        gridOriginX:EPHGRID.origin.x/US,gridOriginY:EPHGRID.origin.y/US,gridOriginZ:EPHGRID.origin.z/US,
        gridAngle:EPHGRID.angle}});
      statusDiv.textContent='Point #'+vSel.id+' supprimé';
      vSel={{type:null,id:null,oid:null}};
    }} else if(vSel.type==='segment'){{
      sendAction({{type:'delete_segment',id:vSel.id,
        gridOriginX:EPHGRID.origin.x/US,gridOriginY:EPHGRID.origin.y/US,gridOriginZ:EPHGRID.origin.z/US,
        gridAngle:EPHGRID.angle}});
      statusDiv.textContent='Segment #'+vSel.id+' supprimé';
      vSel={{type:null,id:null,oid:null}};
    }}
    ev.preventDefault();
  }}

  // Escape → dismiss grid
  if(ev.key==='Escape'){{
    dismissGrid();
    statusDiv.textContent='Grille fermée';
  }}
}});

// ════════════════════════════════════════════════════════════════════
//  RENDER LOOP
// ════════════════════════════════════════════════════════════════════
(function loop(){{ requestAnimationFrame(loop); renderer.render(scene,camera); }})();

// ── Resize ────────────────────────────────────────────────────────────
new ResizeObserver(()=>{{
  const nw=wrap.clientWidth;
  renderer.setSize(nw,{height}); camera.aspect=nw/{height}; camera.updateProjectionMatrix();
}}).observe(wrap);
</script>
</body></html>"""

    st.markdown('<div class="viewer-wrap">', unsafe_allow_html=True)
    st_html(html, height=height+4, scrolling=False)
    st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PLAN EDITOR PANEL
# ─────────────────────────────────────────────────────────────────────────────
def _obj_idx(obj_df, oid):
    m=obj_df.index[obj_df["object_id"]==oid]; return m[0] if len(m) else None

def panel_plan_editor(obj_df, pts_df, seg_df, sel_oid, coincident_ids):

    # ── Pending object placement ──────────────────────────────────────
    pending=st.session_state.get("pending_place")
    if pending:
        st.markdown(
            f'<div class="pending-box">📍 <strong>Placement en attente</strong> : '
            f'X {pending["x"]:.1f} · Y {pending["y"]:.1f} · Z {pending["z"]:.1f} cm<br>'
            f'Sélectionnez un objet puis cliquez "Placer ici".</div>',
            unsafe_allow_html=True)
        if sel_oid is not None:
            row=obj_df[obj_df["object_id"]==sel_oid]
            if not row.empty:
                o=row.iloc[0]
                ax,ay,az=float(o.get("anchor_x",0)),float(o.get("anchor_y",0)),float(o.get("anchor_z",0))
                npx=pending["x"]-ax; npy=pending["y"]-ay; npz=pending["z"]-az
                st.markdown(f'<div class="info-box">Ancre objet : ({ax:.1f},{ay:.1f},{az:.1f}) cm → position objet : ({npx:.1f},{npy:.1f},{npz:.1f}) cm</div>', unsafe_allow_html=True)
                if st.button("📦 Placer l'objet ici (ancre → nœud grille)", key="do_place_obj"):
                    idx=_obj_idx(obj_df,sel_oid)
                    if idx is not None:
                        df2=obj_df.copy()
                        df2.at[idx,"pos_x"]=npx; df2.at[idx,"pos_y"]=npy; df2.at[idx,"pos_z"]=npz
                        save_parquet(df2,OBJ_KEY)
                    st.session_state["pending_place"]=None; st.rerun()
        c1,c2=st.columns(2)
        if c2.button("❌ Annuler", key="cancel_place"):
            st.session_state["pending_place"]=None; st.rerun()
        st.divider()

    if coincident_ids:
        n=len(coincident_ids)
        st.markdown(f'<div class="pending-box" style="background:#2a1a1a;border-color:#5a2a2a;color:#f78166">'
                    f'⚠️ <strong>{n} points coïncidents</strong> détectés (rouge dans la vue)</div>',
                    unsafe_allow_html=True)

    if sel_oid is None:
        st.markdown('<div class="info-box">👆 Cliquez un objet dans la vue 3D ou choisissez-en un dans la liste.</div>',unsafe_allow_html=True)
        return

    row=obj_df[obj_df["object_id"]==sel_oid]
    if row.empty: return
    obj=row.iloc[0]
    px,py,pz=float(obj["pos_x"]),float(obj["pos_y"]),float(obj["pos_z"])
    qx,qy,qz,qw=float(obj["rot_x"]),float(obj["rot_y"]),float(obj["rot_z"]),float(obj["rot_w"])
    sx,sy,sz=float(obj["scale_x"]),float(obj["scale_y"]),float(obj["scale_z"])
    ex,ey,ez=quat_to_euler(qx,qy,qz,qw)
    n_pts=len(pts_df[pts_df["object_id"]==sel_oid]) if not pts_df.empty else 0
    n_segs=len(seg_df[seg_df["object_id"]==sel_oid])  if not seg_df.empty else 0

    st.markdown(
        f'<div class="metric-row">'
        f'<div class="metric-card"><div class="metric-val">{obj["name"]}</div><div class="metric-lbl">Objet</div></div>'
        f'<div class="metric-card"><div class="metric-val">{n_pts}</div><div class="metric-lbl">Points</div></div>'
        f'<div class="metric-card"><div class="metric-val">{n_segs}</div><div class="metric-lbl">Segments</div></div>'
        f'</div>',unsafe_allow_html=True)
    st.markdown(
        f'<div class="pos-display">'
        f'<div class="pos-axis"><span>X</span>{px:.1f}</div>'
        f'<div class="pos-axis"><span>Y</span>{py:.1f}</div>'
        f'<div class="pos-axis"><span>Z</span>{pz:.1f}</div>'
        f'<div class="pos-axis" style="margin-left:8px"><span>RY</span>{ey:.1f}°</div>'
        f'</div>',unsafe_allow_html=True)

    tabs=st.tabs(["🕹 Déplacer","🔄 Pivoter","📐 Échelle","⚓ Ancre","📍 Exact","↗ Aligner","🗑 Suppr"])

    # ── DÉPLACER ─────────────────────────────────────────────────────
    with tabs[0]:
        c1,_=st.columns([3,1])
        step=c1.number_input("Pas (cm)",min_value=0.1,max_value=9999.0,
            value=st.session_state["move_step"],step=0.1,format="%.1f",key="v_ms")
        st.session_state["move_step"]=step

        def _mv(dx=0,dy=0,dz=0):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is None: return
            df2=obj_df.copy(); df2.at[idx,"pos_x"]+=dx; df2.at[idx,"pos_y"]+=dy; df2.at[idx,"pos_z"]+=dz
            save_parquet(df2,OBJ_KEY); st.rerun()

        st.markdown('<p class="move-lbl">Horizontal X/Z</p>',unsafe_allow_html=True)
        _,tc,_=st.columns([1,1,1])
        if tc.button("⬆ −Z",key="m_mz",use_container_width=True): _mv(dz=-step)
        l,mc,r=st.columns(3)
        if l.button("◀ −X",key="m_mx",use_container_width=True): _mv(dx=-step)
        mc.markdown(f"<div style='text-align:center;padding:10px 0;border:1px solid #21262d;border-radius:5px;font-size:10px;color:#888'>X {px:.1f}<br>Z {pz:.1f}</div>",unsafe_allow_html=True)
        if r.button("▶ +X",key="m_px",use_container_width=True): _mv(dx=+step)
        _,bc,_=st.columns([1,1,1])
        if bc.button("⬇ +Z",key="m_pz",use_container_width=True): _mv(dz=+step)
        st.markdown('<p class="move-lbl">Vertical Y</p>',unsafe_allow_html=True)
        y1,y2,y3=st.columns(3)
        if y1.button("▲ +Y",key="m_py",use_container_width=True): _mv(dy=+step)
        y2.markdown(f"<div style='text-align:center;padding:6px 0;font-size:10px;color:#888'>Y {py:.1f}</div>",unsafe_allow_html=True)
        if y3.button("▼ −Y",key="m_my",use_container_width=True): _mv(dy=-step)

    # ── PIVOTER ──────────────────────────────────────────────────────
    with tabs[1]:
        rstep=st.number_input("Pas (°)",min_value=0.1,max_value=180.0,
            value=st.session_state["rot_step"],step=0.5,format="%.1f",key="v_rs")
        st.session_state["rot_step"]=rstep

        def _rot(axis,deg):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is None: return
            df2=obj_df.copy()
            nx,ny,nz,nw=compose_rot(float(df2.at[idx,"rot_x"]),float(df2.at[idx,"rot_y"]),
                                     float(df2.at[idx,"rot_z"]),float(df2.at[idx,"rot_w"]),axis,deg)
            df2.at[idx,"rot_x"]=nx;df2.at[idx,"rot_y"]=ny;df2.at[idx,"rot_z"]=nz;df2.at[idx,"rot_w"]=nw
            save_parquet(df2,OBJ_KEY); st.rerun()

        for lbl2,axis in [("Axe Y (horizontal)","y"),("Axe X (tilt)","x"),("Axe Z (roulis)","z")]:
            st.markdown(f'<p class="move-lbl">{lbl2}</p>',unsafe_allow_html=True)
            c1,c2=st.columns(2)
            if c1.button(f"↺ −{rstep:.1f}°",key=f"r{axis}m",use_container_width=True): _rot(axis,-rstep)
            if c2.button(f"↻ +{rstep:.1f}°",key=f"r{axis}p",use_container_width=True): _rot(axis,+rstep)

        st.markdown(f'<div class="info-box">RX {ex:.1f}° &nbsp; RY {ey:.1f}° &nbsp; RZ {ez:.1f}°<br>'
                    f'💡 Cliquer rapidement pour rotation continue.</div>',unsafe_allow_html=True)
        if st.button("⟲ Réinitialiser rotation",key="rot_reset"):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is not None:
                df2=obj_df.copy()
                df2.at[idx,"rot_x"]=0;df2.at[idx,"rot_y"]=0;df2.at[idx,"rot_z"]=0;df2.at[idx,"rot_w"]=1
                save_parquet(df2,OBJ_KEY); st.rerun()

    # ── ÉCHELLE ──────────────────────────────────────────────────────
    with tabs[2]:
        sstep=st.number_input("Pas",min_value=0.01,max_value=100.0,
            value=st.session_state["scale_step"],step=0.05,format="%.2f",key="v_ss")
        st.session_state["scale_step"]=sstep
        unif=st.checkbox("Uniforme",value=True,key="scl_u")

        def _scl(ds,axis=None):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is None: return
            df2=obj_df.copy()
            axes=["scale_x","scale_y","scale_z"] if unif else ([f"scale_{axis}"] if axis else ["scale_x"])
            for a in axes: df2.at[idx,a]=max(0.01,float(df2.at[idx,a])+ds)
            save_parquet(df2,OBJ_KEY); st.rerun()

        if unif:
            c1,c2=st.columns(2)
            if c1.button(f"▲ +{sstep:.2f}",key="su_p",use_container_width=True): _scl(+sstep)
            if c2.button(f"▼ −{sstep:.2f}",key="su_m",use_container_width=True): _scl(-sstep)
        else:
            for ll,ax in [("X","x"),("Y","y"),("Z","z")]:
                c1,c2=st.columns(2); 
                if c1.button(f"▲{ll}",key=f"s{ax}p",use_container_width=True): _scl(+sstep,ax)
                if c2.button(f"▼{ll}",key=f"s{ax}m",use_container_width=True): _scl(-sstep,ax)

    # ── ANCRE ─────────────────────────────────────────────────────────
    with tabs[3]:
        ax_=float(obj.get("anchor_x",0)); ay_=float(obj.get("anchor_y",0)); az_=float(obj.get("anchor_z",0))
        st.markdown(f'<div class="info-box">Point d\'ancrage actuel (local) :<br>'
                    f'X {ax_:.1f} · Y {ay_:.1f} · Z {az_:.1f} cm<br><br>'
                    f'Le point vert 🟢 dans la vue correspond à l\'ancre de l\'objet sélectionné.<br>'
                    f'Lors d\'un placement via grille, l\'ancre sera positionnée sur le nœud cliqué.</div>',
                    unsafe_allow_html=True)

        o_pts=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
        if not o_pts.empty:
            pt_map={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":
                    (float(r["x"]),float(r["y"]),float(r["z"])) for _,r in o_pts.iterrows()}
            pt_map["Origine de l'objet (0,0,0)"]=(0.0,0.0,0.0)
            chosen=st.selectbox("Choisir l'ancre parmi les points",list(pt_map.keys()),key="anchor_pick")
            if st.button("Définir comme ancre",key="set_anchor"):
                px2,py2,pz2=pt_map[chosen]
                idx=_obj_idx(obj_df,sel_oid)
                if idx is not None:
                    df2=obj_df.copy(); df2.at[idx,"anchor_x"]=px2; df2.at[idx,"anchor_y"]=py2; df2.at[idx,"anchor_z"]=pz2
                    save_parquet(df2,OBJ_KEY); st.success("Ancre mise à jour !"); st.rerun()
        else:
            st.info("Aucun point dans cet objet. Ancre = (0,0,0).")

        st.divider()
        st.markdown("**Définir ancre manuellement**")
        c1,c2,c3=st.columns(3)
        nax=c1.number_input("X",value=ax_,step=1.0,format="%.1f",key=f"an_x{sel_oid}")
        nay=c2.number_input("Y",value=ay_,step=1.0,format="%.1f",key=f"an_y{sel_oid}")
        naz=c3.number_input("Z",value=az_,step=1.0,format="%.1f",key=f"an_z{sel_oid}")
        if st.button("Appliquer ancre manuelle",key="set_anchor_manual"):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is not None:
                df2=obj_df.copy(); df2.at[idx,"anchor_x"]=nax; df2.at[idx,"anchor_y"]=nay; df2.at[idx,"anchor_z"]=naz
                save_parquet(df2,OBJ_KEY); st.rerun()

    # ── POSITION EXACTE ───────────────────────────────────────────────
    with tabs[4]:
        c1,c2,c3=st.columns(3)
        npx=c1.number_input("X(cm)",value=px,step=1.0,format="%.1f",key=f"apx{sel_oid}")
        npy=c2.number_input("Y(cm)",value=py,step=1.0,format="%.1f",key=f"apy{sel_oid}")
        npz=c3.number_input("Z(cm)",value=pz,step=1.0,format="%.1f",key=f"apz{sel_oid}")
        if st.button("Appliquer position",key="abs_pos"):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is not None:
                df2=obj_df.copy(); df2.at[idx,"pos_x"]=npx; df2.at[idx,"pos_y"]=npy; df2.at[idx,"pos_z"]=npz
                save_parquet(df2,OBJ_KEY); st.rerun()
        st.divider()
        c1,c2,c3=st.columns(3)
        nrx=c1.number_input("X°",value=round(ex,2),step=1.0,format="%.2f",key=f"arx{sel_oid}")
        nry=c2.number_input("Y°",value=round(ey,2),step=1.0,format="%.2f",key=f"ary{sel_oid}")
        nrz=c3.number_input("Z°",value=round(ez,2),step=1.0,format="%.2f",key=f"arz{sel_oid}")
        if st.button("Appliquer rotation",key="abs_rot"):
            aq=euler_to_quat(nrx,nry,nrz)
            idx=_obj_idx(obj_df,sel_oid)
            if idx is not None:
                df2=obj_df.copy(); df2.at[idx,"rot_x"]=aq[0]; df2.at[idx,"rot_y"]=aq[1]
                df2.at[idx,"rot_z"]=aq[2]; df2.at[idx,"rot_w"]=aq[3]; save_parquet(df2,OBJ_KEY); st.rerun()

    # ── ALIGNER ───────────────────────────────────────────────────────
    with tabs[5]:
        st.markdown('<div class="info-box"><b>Alignement par point de référence</b><br>'
                    'Choisissez un point de l\'objet sélectionné comme référence, puis une position cible.<br>'
                    'L\'objet entier sera déplacé pour que le point de référence soit à la position cible.</div>',
                    unsafe_allow_html=True)

        o_pts=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
        if o_pts.empty:
            st.info("Ajoutez d'abord des points à cet objet.")
        else:
            pt_map2={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":
                     (float(r["x"]),float(r["y"]),float(r["z"])) for _,r in o_pts.iterrows()}
            ref_lbl=st.selectbox("Point de référence (local)",list(pt_map2.keys()),key="align_ref_sel")
            ref_local=pt_map2[ref_lbl]
            st.markdown(f'Point local sélectionné : ({ref_local[0]:.1f}, {ref_local[1]:.1f}, {ref_local[2]:.1f}) cm')

            st.markdown("**Position cible dans le plan (cm)**")
            c1,c2,c3=st.columns(3)
            tgt_x=c1.number_input("Cible X",value=px,step=1.0,format="%.1f",key=f"tgt_x{sel_oid}")
            tgt_y=c2.number_input("Cible Y",value=py,step=1.0,format="%.1f",key=f"tgt_y{sel_oid}")
            tgt_z=c3.number_input("Cible Z",value=pz,step=1.0,format="%.1f",key=f"tgt_z{sel_oid}")

            # Also allow using pending_place as target
            pending=st.session_state.get("pending_place")
            if pending:
                if st.button(f"🎯 Utiliser nœud grille ({pending['x']:.1f},{pending['y']:.1f},{pending['z']:.1f})",key="use_grid_as_target"):
                    tgt_x,tgt_y,tgt_z=pending["x"],pending["y"],pending["z"]

            if st.button("↗ Déplacer l'objet",key="do_align",use_container_width=True):
                # new_obj_pos = target - ref_local (ignore rotation for simplicity)
                new_px=tgt_x-ref_local[0]; new_py=tgt_y-ref_local[1]; new_pz=tgt_z-ref_local[2]
                idx=_obj_idx(obj_df,sel_oid)
                if idx is not None:
                    df2=obj_df.copy(); df2.at[idx,"pos_x"]=new_px; df2.at[idx,"pos_y"]=new_py; df2.at[idx,"pos_z"]=new_pz
                    save_parquet(df2,OBJ_KEY); st.success(f"Objet déplacé → ({new_px:.1f},{new_py:.1f},{new_pz:.1f}) cm"); st.rerun()

    # ── SUPPRIMER ────────────────────────────────────────────────────
    with tabs[6]:
        st.warning(f"⚠️ Supprimer **{obj['name']}** et tous ses points / segments ?")
        if st.button("🗑 Confirmer",key="del_obj_c"):
            df2=obj_df[obj_df["object_id"]!=sel_oid]
            p2=pts_df[pts_df["object_id"]!=sel_oid] if not pts_df.empty else pts_df
            s2=seg_df[seg_df["object_id"]!=sel_oid]  if not seg_df.empty else seg_df
            for d_,k_ in [(df2,OBJ_KEY),(p2,PTS_KEY),(s2,SEG_KEY)]: save_parquet(d_,k_)
            st.session_state["object_id"]=None; st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# OBJECT DESIGNER PANEL
# ─────────────────────────────────────────────────────────────────────────────
def panel_object_designer(obj_df, pts_df, seg_df, sel_oid):

    # ── Pending point from grid ───────────────────────────────────────
    pending=st.session_state.get("pending_pt")
    if pending and sel_oid is not None:
        st.markdown(
            f'<div class="pending-box">📍 <strong>Nœud grille cliqué</strong> : '
            f'X {pending["x"]:.1f} · Y {pending["y"]:.1f} · Z {pending["z"]:.1f} cm</div>',
            unsafe_allow_html=True)
        c1,c2=st.columns(2)
        if c1.button("✅ Créer point ici",key="confirm_grid_pt"):
            pid=next_id(pts_df,"point_id")
            p2=pd.concat([pts_df,pd.DataFrame([{"point_id":pid,"object_id":sel_oid,
                "x":pending["x"],"y":pending["y"],"z":pending["z"]}])],ignore_index=True)
            save_parquet(p2,PTS_KEY); st.session_state["pending_pt"]=None; st.rerun()
        if c2.button("❌ Ignorer",key="cancel_grid_pt"):
            st.session_state["pending_pt"]=None; st.rerun()
        st.divider()

    if sel_oid is None:
        st.markdown('<div class="info-box">👆 Sélectionnez un objet.<br><br>'
                    '🖱 Cliquer un point → le sélectionner<br>'
                    '⌨ <code>Suppr</code> → supprimer la sélection<br>'
                    '🖱 Cliquer le sol → activer la grille éphémère<br>'
                    '🟡 Dial rond → faire tourner la grille (press+drag)<br>'
                    '⌨ <code>Échap</code> → fermer la grille</div>',
                    unsafe_allow_html=True); return

    if obj_df[obj_df["object_id"]==sel_oid].empty: return

    o_pts=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
    o_segs=seg_df[seg_df["object_id"]==sel_oid] if not seg_df.empty else pd.DataFrame()

    tab_pts,tab_segs,tab_csv=st.tabs(["📍 Points","🔗 Segments","⬇ CSV"])

    # ── POINTS ───────────────────────────────────────────────────────
    with tab_pts:
        with st.expander("➕ Ajouter un point",expanded=o_pts.empty):
            c1,c2,c3,c4=st.columns([2,2,2,1])
            nx=c1.number_input("X(cm)",value=0.0,step=1.0,format="%.1f",key="np_x")
            ny=c2.number_input("Y(cm)",value=0.0,step=1.0,format="%.1f",key="np_y")
            nz=c3.number_input("Z(cm)",value=0.0,step=1.0,format="%.1f",key="np_z")
            c4.markdown("<br>",unsafe_allow_html=True)
            if c4.button("OK",key="add_pt"):
                pid=next_id(pts_df,"point_id")
                p2=pd.concat([pts_df,pd.DataFrame([{"point_id":pid,"object_id":sel_oid,"x":float(nx),"y":float(ny),"z":float(nz)}])],ignore_index=True)
                save_parquet(p2,PTS_KEY); st.rerun()

        if o_pts.empty: st.info("Aucun point."); return

        pt_map={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":int(r["point_id"])
                for _,r in o_pts.iterrows()}
        sel_lbl=st.selectbox("Point actif",list(pt_map.keys()),key="sel_pt_lbl")
        sel_pid=pt_map[sel_lbl]
        pt_row=o_pts[o_pts["point_id"]==sel_pid].iloc[0]
        cx,cy,cz=float(pt_row["x"]),float(pt_row["y"]),float(pt_row["z"])

        st.markdown(f'<div class="pos-display"><div class="pos-axis"><span>X</span>{cx:.1f}</div>'
                    f'<div class="pos-axis"><span>Y</span>{cy:.1f}</div>'
                    f'<div class="pos-axis"><span>Z</span>{cz:.1f}</div></div>',unsafe_allow_html=True)

        pstep=st.number_input("Pas (cm)",min_value=0.1,max_value=9999.0,
            value=st.session_state["pt_move_step"],step=0.1,format="%.1f",key="pt_step_v")
        st.session_state["pt_move_step"]=pstep

        def _mpt(dx=0,dy=0,dz=0):
            idx=pts_df.index[pts_df["point_id"]==sel_pid][0]
            df2=pts_df.copy(); df2.at[idx,"x"]+=dx; df2.at[idx,"y"]+=dy; df2.at[idx,"z"]+=dz
            save_parquet(df2,PTS_KEY); st.rerun()

        st.markdown('<p class="move-lbl">Plan XZ</p>',unsafe_allow_html=True)
        _,tc,_=st.columns([1,1,1])
        if tc.button("⬆ −Z",key="pt_mz",use_container_width=True): _mpt(dz=-pstep)
        l2,m2,r2=st.columns(3)
        if l2.button("◀ −X",key="pt_mx",use_container_width=True): _mpt(dx=-pstep)
        m2.markdown(f"<div style='text-align:center;padding:8px 0;border:1px solid #21262d;border-radius:5px;font-size:10px;color:#888'>X{cx:.1f}<br>Z{cz:.1f}</div>",unsafe_allow_html=True)
        if r2.button("▶ +X",key="pt_px",use_container_width=True): _mpt(dx=+pstep)
        _,bc2,_=st.columns([1,1,1])
        if bc2.button("⬇ +Z",key="pt_pz",use_container_width=True): _mpt(dz=+pstep)

        st.markdown('<p class="move-lbl">Vertical Y</p>',unsafe_allow_html=True)
        y1,y2,y3=st.columns(3)
        if y1.button("▲ +Y",key="pt_py",use_container_width=True): _mpt(dy=+pstep)
        y2.markdown(f"<div style='text-align:center;padding:6px 0;font-size:10px;color:#888'>Y {cy:.1f}</div>",unsafe_allow_html=True)
        if y3.button("▼ −Y",key="pt_my",use_container_width=True): _mpt(dy=-pstep)

        st.divider()
        st.markdown("**Édition directe**")
        edit=st.data_editor(o_pts[["point_id","x","y","z"]].reset_index(drop=True),
            key=f"pts_edit_{sel_oid}",use_container_width=True,hide_index=True,
            column_config={"point_id":st.column_config.NumberColumn("ID",disabled=True,width="small"),
                           "x":st.column_config.NumberColumn("X(cm)",step=0.1,format="%.1f"),
                           "y":st.column_config.NumberColumn("Y(cm)",step=0.1,format="%.1f"),
                           "z":st.column_config.NumberColumn("Z(cm)",step=0.1,format="%.1f")})
        c1,c2=st.columns(2)
        if c1.button("💾 Sauvegarder",key="save_pts"):
            df2=pts_df.copy()
            for _,rr in edit.iterrows():
                if pd.notna(rr.get("point_id")):
                    idx=df2.index[df2["point_id"]==int(rr["point_id"])]
                    if len(idx): df2.at[idx[0],"x"]=float(rr["x"]); df2.at[idx[0],"y"]=float(rr["y"]); df2.at[idx[0],"z"]=float(rr["z"])
            save_parquet(df2,PTS_KEY); st.success("Sauvegardé"); st.rerun()
        if c2.button("🗑 Supprimer ce point",key="del_pt"):
            p2=pts_df[pts_df["point_id"]!=sel_pid]
            s2=seg_df[(seg_df["point_a_id"]!=sel_pid)&(seg_df["point_b_id"]!=sel_pid)] if not seg_df.empty else seg_df
            save_parquet(p2,PTS_KEY); save_parquet(s2,SEG_KEY); st.rerun()

    # ── SEGMENTS ─────────────────────────────────────────────────────
    with tab_segs:
        if o_pts.empty or len(o_pts)<2: st.info("≥2 points requis."); return
        pt_lbl={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":int(r["point_id"])
                for _,r in o_pts.iterrows()}
        lbls=list(pt_lbl.keys())
        c1,c2=st.columns(2)
        sa=c1.selectbox("Point A",lbls,key="seg_a"); sb=c2.selectbox("Point B",lbls,key="seg_b",index=min(1,len(lbls)-1))
        if st.button("🔗 Créer segment",key="mk_seg"):
            pa_id,pb_id=pt_lbl[sa],pt_lbl[sb]
            if pa_id==pb_id: st.error("Deux points distincts requis.")
            else:
                dupe=(not seg_df.empty) and not seg_df[
                    (seg_df["object_id"]==sel_oid)&
                    (((seg_df["point_a_id"]==pa_id)&(seg_df["point_b_id"]==pb_id))|
                     ((seg_df["point_a_id"]==pb_id)&(seg_df["point_b_id"]==pa_id)))].empty
                if dupe: st.warning("Segment déjà existant.")
                else:
                    sid=next_id(seg_df,"segment_id")
                    s2=pd.concat([seg_df,pd.DataFrame([{"segment_id":sid,"object_id":sel_oid,"point_a_id":pa_id,"point_b_id":pb_id}])],ignore_index=True)
                    save_parquet(s2,SEG_KEY); st.rerun()
        if not o_segs.empty:
            st.markdown(f"**{len(o_segs)} segment(s)**")
            st.dataframe(o_segs[["segment_id","point_a_id","point_b_id"]].reset_index(drop=True),use_container_width=True,hide_index=True)
            c1,c2=st.columns([3,1])
            dsid=c1.selectbox("Supprimer",o_segs["segment_id"].tolist(),key="dseg_sel")
            if c2.button("🗑",key="dseg_btn"): save_parquet(seg_df[seg_df["segment_id"]!=dsid],SEG_KEY); st.rerun()

    # ── CSV ───────────────────────────────────────────────────────────
    with tab_csv:
        st.markdown('<div class="info-box">Format : <code>x,y,z</code> par ligne (cm)</div>',unsafe_allow_html=True)
        up=st.file_uploader("CSV",type=["csv"],key="csv_up")
        if up:
            try:
                dfc=pd.read_csv(up,names=["x","y","z"]); st.dataframe(dfc.head(10),use_container_width=True)
                st.markdown(f"**{len(dfc)} points**")
                if st.button("⬇ Importer",key="do_import"):
                    base=next_id(pts_df,"point_id")
                    new=[{"point_id":base+i,"object_id":sel_oid,"x":float(r["x"]),"y":float(r["y"]),"z":float(r["z"])}
                         for i,(_,r) in enumerate(dfc.iterrows())]
                    save_parquet(pd.concat([pts_df,pd.DataFrame(new)],ignore_index=True),PTS_KEY)
                    st.success(f"{len(new)} points !"); st.rerun()
            except Exception as e: st.error(f"Erreur : {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    init_session()

    # ── R2 init ──────────────────────────────────────────────────────
    if not st.session_state["r2_ready"]:
        try: init_r2_tables(); st.session_state["r2_ready"]=True
        except Exception as e: st.warning(f"R2 : {e}")

    # ── Load data ─────────────────────────────────────────────────────
    proj_df=load_parquet(PROJ_KEY,PROJ_COLS)
    obj_df=load_objects()
    pts_df=load_parquet(PTS_KEY,PTS_COLS)
    seg_df=load_parquet(SEG_KEY,SEG_COLS)

    # ── Message bus input (hidden, found by JS via placeholder) ───────
    viewer_msg=st.text_input("",key="_viewer_msg",placeholder="__3ds__",label_visibility="collapsed")
    if viewer_msg and viewer_msg.startswith("{") and viewer_msg!="{}":
        process_viewer_action(viewer_msg, obj_df, pts_df, seg_df)
        st.session_state["_viewer_msg"]=""

    # ── Sidebar ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<div class="studio-header"><div><div class="studio-title">🧊 3D Design Studio</div>'
                    '<div class="studio-sub">Point · Segment · Transform</div></div></div>',unsafe_allow_html=True)

        mode_lbl=st.radio("Mode",["📐 Plan Editor","✏️ Object Designer"],
            index=0 if st.session_state["mode"]=="plan_editor" else 1,
            horizontal=True,label_visibility="collapsed")
        st.session_state["mode"]="plan_editor" if "Plan" in mode_lbl else "object_designer"
        bcls="badge-plan" if st.session_state["mode"]=="plan_editor" else "badge-object"
        blbl="PLAN EDITOR" if st.session_state["mode"]=="plan_editor" else "OBJECT DESIGNER"
        st.markdown(f'<span class="badge {bcls}">{blbl}</span>',unsafe_allow_html=True)
        st.divider()

        # Projets
        st.markdown('<p class="section-label">📁 Projets</p>',unsafe_allow_html=True)
        with st.expander("Nouveau projet",expanded=proj_df.empty):
            pname=st.text_input("Nom",key="new_proj_name",placeholder="Mon projet…")
            if st.button("Créer",key="create_proj"):
                if pname.strip():
                    pid=next_id(proj_df,"project_id")
                    proj_df=pd.concat([proj_df,pd.DataFrame([{"project_id":pid,"name":pname.strip(),"created_at":datetime.now().isoformat()}])],ignore_index=True)
                    save_parquet(proj_df,PROJ_KEY); st.session_state["project_id"]=pid; st.session_state["object_id"]=None; st.rerun()
        if not proj_df.empty:
            pnames=proj_df["name"].tolist(); pids=proj_df["project_id"].tolist()
            cur=st.session_state["project_id"]; ci=pids.index(cur) if cur in pids else 0
            sn=st.selectbox("Projet",pnames,index=ci,key="proj_sel",label_visibility="collapsed")
            st.session_state["project_id"]=pids[pnames.index(sn)]
            if st.button("🗑 Supprimer projet",key="del_proj"):
                dpid=st.session_state["project_id"]; proj_df=proj_df[proj_df["project_id"]!=dpid]
                if not obj_df.empty:
                    doids=obj_df[obj_df["project_id"]==dpid]["object_id"].tolist()
                    obj_df=obj_df[obj_df["project_id"]!=dpid]
                    if not pts_df.empty: pts_df=pts_df[~pts_df["object_id"].isin(doids)]
                    if not seg_df.empty: seg_df=seg_df[~seg_df["object_id"].isin(doids)]
                for d_,k_ in [(proj_df,PROJ_KEY),(obj_df,OBJ_KEY),(pts_df,PTS_KEY),(seg_df,SEG_KEY)]: save_parquet(d_,k_)
                st.session_state["project_id"]=None; st.session_state["object_id"]=None; st.rerun()
        else: st.caption("Aucun projet.")

        st.divider()

        # Objets
        cur_pid=st.session_state.get("project_id")
        st.markdown('<p class="section-label">📦 Objets</p>',unsafe_allow_html=True)
        if cur_pid is not None:
            with st.expander("Nouvel objet"):
                oname=st.text_input("Nom",key="new_obj_name",placeholder="Objet A…")
                if st.button("Créer",key="create_obj"):
                    oid=next_id(obj_df,"object_id")
                    obj_df=pd.concat([obj_df,pd.DataFrame([{"object_id":oid,"project_id":cur_pid,
                        "name":oname.strip() or f"Objet {oid}","pos_x":0.,"pos_y":0.,"pos_z":0.,
                        "rot_x":0.,"rot_y":0.,"rot_z":0.,"rot_w":1.,"scale_x":1.,"scale_y":1.,"scale_z":1.,
                        "anchor_x":0.,"anchor_y":0.,"anchor_z":0.}])],ignore_index=True)
                    save_parquet(obj_df,OBJ_KEY); st.session_state["object_id"]=oid; st.rerun()
            proj_objs=obj_df[obj_df["project_id"]==cur_pid] if not obj_df.empty else pd.DataFrame()
            sel_oid=st.session_state.get("object_id")
            if not proj_objs.empty:
                for _,o in proj_objs.iterrows():
                    oid2=int(o["object_id"]); active=oid2==sel_oid
                    np_=len(pts_df[pts_df["object_id"]==oid2]) if not pts_df.empty else 0
                    if st.button(f"{'▶ ' if active else '  '}{o['name']} · {np_}pt",key=f"sel_{oid2}",use_container_width=True):
                        st.session_state["object_id"]=oid2; st.rerun()
            else: st.caption("Aucun objet.")
        else: st.caption("Sélectionnez un projet.")

        st.divider()

        # Affichage
        st.markdown('<p class="section-label">👁 Affichage</p>',unsafe_allow_html=True)
        c1,c2=st.columns(2)
        st.session_state["show_grid"]=c1.checkbox("Grille",value=True)
        st.session_state["show_axes"]=c2.checkbox("Axes",value=True)
        st.session_state["snap"]=st.checkbox("Snap visuel",value=True)
        if st.session_state["snap"]:
            st.session_state["snap_dist"]=st.slider("Seuil snap",0.5,30.0,5.0,0.5,label_visibility="collapsed")

        st.divider()
        # Grille éphémère
        st.markdown('<p class="section-label">⊞ Grille éphémère</p>',unsafe_allow_html=True)
        st.session_state["grid_cell_size"]=st.number_input(
            "Pas de grille (cm)",min_value=0.5,max_value=500.0,
            value=st.session_state["grid_cell_size"],step=0.5,format="%.1f")
        st.session_state["grid_extent"]=st.slider("Étendue (cellules)",2,20,st.session_state["grid_extent"])
        go=st.session_state.get("grid_origin")
        if go:
            st.markdown(f'<span style="font-size:10px;color:#3fb950">Grille active : ({go["x"]:.1f},{go["z"]:.1f}) cm · {st.session_state["grid_angle"]:.1f}°</span>',unsafe_allow_html=True)
            if st.button("✕ Fermer grille",key="close_grid"):
                st.session_state["grid_origin"]=None; st.rerun()

    # ── Main zone ─────────────────────────────────────────────────────
    cur_oid=st.session_state.get("object_id")
    cur_pts=st.session_state.get("selected_pts",[])
    cur_pid=st.session_state.get("project_id")

    # Coincident detection (only in PE mode and when moving)
    coincident_ids=set()
    if st.session_state["mode"]=="plan_editor" and not obj_df.empty and not pts_df.empty:
        coincident_ids=find_coincident_points(obj_df,pts_df,threshold_cm=0.5)

    scene=build_scene_json(cur_pid,obj_df,pts_df,seg_df,cur_oid,cur_pts,coincident_ids)
    render_viewer(scene,st.session_state["mode"],height=530)

    st.markdown("<hr style='border-color:#21262d;margin:8px 0'>",unsafe_allow_html=True)

    if st.session_state["mode"]=="plan_editor":
        panel_plan_editor(obj_df,pts_df,seg_df,cur_oid,coincident_ids)
    else:
        panel_object_designer(obj_df,pts_df,seg_df,cur_oid)


if __name__=="__main__":
    main()
