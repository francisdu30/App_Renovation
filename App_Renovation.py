"""
3D Design Studio v4
Streamlit + Three.js + Cloudflare R2 (Parquet)
Grille éphémère VERTICALE — pas de sphères, lignes seulement
Hover = intersection plan + snapping, indicateur temporaire
"""

import io, json, math
from datetime import datetime
import boto3, pandas as pd
import streamlit as st
from streamlit.components.v1 import html as st_html

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG + CSS
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="3D Design Studio", page_icon="🧊",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;700;800&display=swap');
html,body,[class*="css"]{font-family:'JetBrains Mono',monospace;}
:root{--bg0:#0a0c10;--bg1:#0f1117;--bg2:#161b22;--bg3:#1c2333;
      --border:#21262d;--accent:#1a73e8;--text0:#e6edf3;--text1:#8b949e;--text2:#484f58;}
.stApp{background:var(--bg0);}
section[data-testid="stSidebar"]{background:var(--bg1)!important;border-right:1px solid var(--border);}
section[data-testid="stSidebar"]>div{padding-top:.4rem;}
.main .block-container{padding:.6rem 1rem 1rem 1rem;max-width:100%;}

.studio-header{display:flex;align-items:center;gap:10px;padding:10px 0 6px 0;
  border-bottom:1px solid var(--border);margin-bottom:10px;}
.studio-title{font-family:'Syne',sans-serif;font-size:17px;font-weight:800;
  color:var(--accent);letter-spacing:-.5px;}
.studio-sub{font-size:9px;color:var(--text2);letter-spacing:2px;text-transform:uppercase;}

.badge{display:inline-block;padding:2px 8px;border-radius:4px;
  font-size:10px;font-weight:600;letter-spacing:1px;text-transform:uppercase;}
.badge-plan  {background:#1a2744;color:#58a6ff;border:1px solid #1f3a72;}
.badge-object{background:#2a1a1a;color:#f78166;border:1px solid #5a2a2a;}
.section-label{font-size:9px;letter-spacing:2px;text-transform:uppercase;
  color:var(--text2);margin:6px 0 3px 0;}

.metric-row{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:6px 0;}
.metric-card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;
  padding:7px 10px;text-align:center;}
.metric-val{font-size:15px;font-weight:700;color:var(--accent);}
.metric-lbl{font-size:9px;color:var(--text2);letter-spacing:1px;text-transform:uppercase;}

.pos-display{background:var(--bg2);border:1px solid var(--border);border-radius:6px;
  padding:7px 12px;margin:5px 0;display:flex;gap:14px;align-items:center;flex-wrap:wrap;}
.pos-axis{font-size:11px;}
.pos-axis span{color:var(--text2);font-size:9px;text-transform:uppercase;margin-right:3px;}
.move-lbl{font-size:9px;color:var(--text2);letter-spacing:1.5px;text-transform:uppercase;
  margin-bottom:2px;margin-top:6px;}

/* Grid control bar */
.grid-bar{background:#0d1929;border:1px solid #1f3a72;border-radius:8px;
  padding:10px 14px;margin:6px 0;}
.grid-bar-title{font-size:9px;color:#58a6ff;letter-spacing:2px;
  text-transform:uppercase;margin-bottom:6px;}

.pending-box{background:#0d1f0d;border:1px solid #2a5a2a;border-radius:6px;
  padding:9px 12px;font-size:11px;color:#6ab06a;margin:6px 0;}
.pending-box strong{color:#3fb950;}
.info-box{background:var(--bg2);border:1px solid var(--border);border-radius:6px;
  padding:9px 12px;font-size:11px;color:var(--text1);margin:6px 0;}
.info-box code{background:var(--bg3);padding:1px 4px;border-radius:3px;
  color:#3fb950;font-size:10px;}
.viewer-wrap{border-radius:8px;overflow:hidden;border:1px solid var(--border);}

/* Hide message bus */
div[data-testid="stTextInput"]:has(input[placeholder="__3ds__"]){
  position:absolute!important;opacity:0!important;pointer-events:none!important;
  width:1px!important;height:1px!important;overflow:hidden!important;}

.stButton>button{background:var(--bg2)!important;border:1px solid var(--border)!important;
  color:var(--text0)!important;font-family:'JetBrains Mono',monospace!important;
  font-size:11px!important;border-radius:5px!important;transition:all .15s!important;}
.stButton>button:hover{border-color:var(--accent)!important;color:var(--accent)!important;}
.stTabs [data-baseweb="tab"]{font-family:'JetBrains Mono',monospace;font-size:11px;}
div[data-testid="stNumberInput"] input{font-family:'JetBrains Mono',monospace;font-size:12px;
  background:var(--bg2)!important;border-color:var(--border)!important;color:var(--text0)!important;}
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
        aws_secret_access_key=st.secrets["R2_SECRET_KEY"], region_name="auto")

def load_parquet(key, cols):
    try:
        obj=get_r2().get_object(Bucket=st.secrets["R2_BUCKET"],Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except: return pd.DataFrame(columns=cols)

def save_parquet(df, key):
    buf=io.BytesIO(); df.to_parquet(buf,index=False,compression="zstd"); buf.seek(0)
    get_r2().put_object(Bucket=st.secrets["R2_BUCKET"],Key=key,Body=buf.getvalue())

PROJ_KEY="projects.parquet"; OBJ_KEY="objects.parquet"
PTS_KEY="points.parquet";    SEG_KEY="segments.parquet"
PROJ_COLS=["project_id","name","created_at"]
OBJ_COLS_BASE=["object_id","project_id","name","pos_x","pos_y","pos_z",
               "rot_x","rot_y","rot_z","rot_w","scale_x","scale_y","scale_z"]
OBJ_COLS_EXT={"anchor_x":0.0,"anchor_y":0.0,"anchor_z":0.0}
PTS_COLS=["point_id","object_id","x","y","z"]
SEG_COLS=["segment_id","object_id","point_a_id","point_b_id"]

def load_objects():
    df=load_parquet(OBJ_KEY,OBJ_COLS_BASE)
    for c,v in OBJ_COLS_EXT.items():
        if c not in df.columns: df[c]=v
    return df

def next_id(df,col):
    if df.empty or col not in df.columns or df[col].isnull().all(): return 1
    return int(df[col].max())+1

def init_r2_tables():
    for key,cols in [(PROJ_KEY,PROJ_COLS),(OBJ_KEY,OBJ_COLS_BASE),(PTS_KEY,PTS_COLS),(SEG_KEY,SEG_COLS)]:
        if load_parquet(key,cols).empty:
            try: save_parquet(pd.DataFrame(columns=cols),key)
            except: pass

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────
def _ss(k,v):
    if k not in st.session_state: st.session_state[k]=v

def init_session():
    _ss("mode","plan_editor"); _ss("project_id",None); _ss("object_id",None)
    _ss("selected_pts",[]); _ss("show_grid",True); _ss("show_axes",True)
    _ss("snap",True); _ss("snap_dist",5.0); _ss("r2_ready",False)
    _ss("move_step",1.0); _ss("rot_step",5.0); _ss("scale_step",0.1); _ss("pt_move_step",1.0)
    # Grille éphémère
    _ss("grid_cell_size",10.0)   # cm par côté de carré
    _ss("grid_extent",8)          # nb de cellules de chaque côté
    _ss("grid_origin",None)       # {x,y,z} cm — persiste entre reruns
    _ss("grid_angle",0)           # degrés (int) — persiste entre reruns
    # Pending
    _ss("pending_pt",None)
    _ss("pending_place",None)

# ─────────────────────────────────────────────────────────────────────────────
# MATH
# ─────────────────────────────────────────────────────────────────────────────
def quat_to_euler(qx,qy,qz,qw):
    sinr=2*(qw*qx+qy*qz); cosr=1-2*(qx*qx+qy*qy)
    ex=math.degrees(math.atan2(sinr,cosr))
    sinp=2*(qw*qy-qz*qx); ey=math.degrees(math.asin(max(-1,min(1,sinp))))
    siny=2*(qw*qz+qx*qy); cosy=1-2*(qy*qy+qz*qz)
    ez=math.degrees(math.atan2(siny,cosy)); return ex,ey,ez

def euler_to_quat(ex,ey,ez):
    rx,ry,rz=math.radians(ex),math.radians(ey),math.radians(ez)
    cy,sy=math.cos(rz/2),math.sin(rz/2); cp,sp=math.cos(ry/2),math.sin(ry/2)
    cr,sr=math.cos(rx/2),math.sin(rx/2)
    return (sr*cp*cy-cr*sp*sy,cr*sp*cy+sr*cp*sy,cr*cp*sy-sr*sp*cy,cr*cp*cy+sr*sp*sy)

def compose_rot(qx,qy,qz,qw,axis,deg):
    a=math.radians(deg)/2; c,s=math.cos(a),math.sin(a)
    dq={"x":(s,0,0,c),"y":(0,s,0,c),"z":(0,0,s,c)}[axis]; dx,dy,dz,dw=dq
    return (dw*qx+dx*qw+dy*qz-dz*qy,dw*qy-dx*qz+dy*qw+dz*qx,
            dw*qz+dx*qy-dy*qx+dz*qw,dw*qw-dx*qx-dy*qy-dz*qz)

def find_coincident_points(obj_df,pts_df,thr=0.5):
    if pts_df.empty or obj_df.empty or len(pts_df)<2: return set()
    world=[]
    for _,pt in pts_df.iterrows():
        oid=int(pt["object_id"]); rows=obj_df[obj_df["object_id"]==oid]
        if rows.empty: continue
        o=rows.iloc[0]
        world.append((int(pt["point_id"]),oid,
                      float(pt["x"])+float(o["pos_x"]),
                      float(pt["y"])+float(o["pos_y"]),
                      float(pt["z"])+float(o["pos_z"])))
    coinc=set(); t2=thr**2
    for i in range(len(world)):
        for j in range(i+1,len(world)):
            if world[i][1]==world[j][1]: continue
            dx=world[i][2]-world[j][2]; dy=world[i][3]-world[j][3]; dz=world[i][4]-world[j][4]
            if dx*dx+dy*dy+dz*dz<t2: coinc.add(world[i][0]); coinc.add(world[j][0])
    return coinc

# ─────────────────────────────────────────────────────────────────────────────
# SCENE JSON
# ─────────────────────────────────────────────────────────────────────────────
def build_scene_json(project_id,obj_df,pts_df,seg_df,sel_obj,sel_pts,coinc_ids):
    go=st.session_state.get("grid_origin")
    scene={
        "objects":[],"showGrid":st.session_state["show_grid"],
        "showAxes":st.session_state["show_axes"],"snap":st.session_state["snap"],
        "snapDist":st.session_state["snap_dist"],"unitScale":0.01,
        "mode":st.session_state["mode"],
        "gridCellSize":float(st.session_state["grid_cell_size"]),
        "gridExtent":int(st.session_state["grid_extent"]),
        "gridOrigin":go,
        "gridAngle":int(st.session_state["grid_angle"]),
        "coincident":list(coinc_ids),
    }
    if project_id is None or obj_df.empty: return scene
    for _,obj in obj_df[obj_df["project_id"]==project_id].iterrows():
        oid=int(obj["object_id"])
        o_pts=pts_df[pts_df["object_id"]==oid] if not pts_df.empty else pd.DataFrame()
        o_seg=seg_df[seg_df["object_id"]==oid]  if not seg_df.empty else pd.DataFrame()
        pts=[{"id":int(p["point_id"]),"x":float(p["x"]),"y":float(p["y"]),"z":float(p["z"]),
              "sel":int(p["point_id"]) in sel_pts,"coin":int(p["point_id"]) in coinc_ids}
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
# VIEWER ACTION PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────
def process_viewer_action(raw,obj_df,pts_df,seg_df):
    try: action=json.loads(raw)
    except: return
    t=action.get("type","")
    if "gridOriginX" in action:
        st.session_state["grid_origin"]={"x":action["gridOriginX"],"y":action["gridOriginY"],"z":action["gridOriginZ"]}
    if "gridAngle" in action:
        st.session_state["grid_angle"]=int(round(float(action["gridAngle"])))%360

    if t=="grid_click_od":
        st.session_state["pending_pt"]={"x":action["x"],"y":action["y"],"z":action["z"]}
    elif t=="grid_click_pe":
        st.session_state["pending_place"]={"x":action["x"],"y":action["y"],"z":action["z"]}
    elif t=="grid_activate":
        st.session_state["grid_origin"]={"x":action["x"],"y":action["y"],"z":action["z"]}
        st.session_state["grid_angle"]=int(round(float(action.get("angle",0))))%360
    elif t=="grid_dismiss":
        st.session_state["grid_origin"]=None
    elif t=="delete_point":
        pid=int(action["id"])
        p2=pts_df[pts_df["point_id"]!=pid]
        s2=seg_df[(seg_df["point_a_id"]!=pid)&(seg_df["point_b_id"]!=pid)] if not seg_df.empty else seg_df
        save_parquet(p2,PTS_KEY); save_parquet(s2,SEG_KEY)
        st.session_state["_viewer_msg"]=""; st.rerun()
    elif t=="delete_segment":
        save_parquet(seg_df[seg_df["segment_id"]!=int(action["id"])],SEG_KEY)
        st.session_state["_viewer_msg"]=""; st.rerun()
    elif t=="select_object":
        st.session_state["object_id"]=int(action["id"])
        st.session_state["_viewer_msg"]=""; st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# VIEWER HTML — Three.js with VERTICAL ephemeral grid
# ─────────────────────────────────────────────────────────────────────────────
def render_viewer(scene,mode,height=530):
    sj=json.dumps(scene)
    is_plan=(mode=="plan_editor")
    bcls="badge-plan" if is_plan else "badge-object"
    blbl="PLAN EDITOR" if is_plan else "OBJECT DESIGNER"

    html_code=f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{background:#fff;overflow:hidden;font-family:'JetBrains Mono',monospace;}}
#wrap{{width:100%;height:{height}px;position:relative;}}
.hud{{position:absolute;pointer-events:none;font-size:10px;}}
#badge{{top:10px;left:10px;padding:4px 10px;border-radius:4px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;}}
.badge-plan{{background:rgba(26,39,68,.9);color:#58a6ff;border:1px solid #1f3a72;}}
.badge-object{{background:rgba(42,26,26,.9);color:#f78166;border:1px solid #5a2a2a;}}
#coords{{bottom:10px;left:10px;color:#333;background:rgba(255,255,255,.92);padding:5px 10px;border-radius:4px;border:1px solid #ccc;font-size:11px;}}
#status{{bottom:10px;right:10px;color:#444;background:rgba(255,255,255,.92);padding:5px 10px;border-radius:4px;border:1px solid #ccc;}}
#help{{top:10px;right:10px;color:#555;background:rgba(255,255,255,.92);padding:8px 12px;border-radius:6px;border:1px solid #ccc;line-height:1.9;font-size:10px;}}
/* Grid info HUD */
#ghud{{top:56px;left:10px;background:rgba(10,18,40,.92);color:#58a6ff;border:1px solid #1f3a72;border-radius:6px;padding:9px 12px;display:none;min-width:180px;font-size:11px;line-height:1.7;}}
#ghud .gt{{font-size:9px;color:#8b949e;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px;}}
#ghud .gd{{color:#3fb950;margin-top:3px;}}
#ghud .gesc{{color:#484f58;font-size:9px;margin-top:4px;}}
/* Hover crosshair */
#crosshair{{display:none;position:absolute;pointer-events:none;}}
</style>
</head>
<body>
<div id="wrap">
  <div id="badge" class="hud {bcls}">{blbl}</div>
  <div id="help" class="hud">
    🖱 Clic droit+glisser → rotation vue<br>
    🖱 Molette+glisser → pan<br>
    🖱 Molette → zoom<br>
    🖱 Clic gauche → sélect / activer grille<br>
    ⌨ Suppr → supprimer sélection (OD)<br>
    ⌨ Échap → fermer grille
  </div>
  <div id="ghud" class="hud">
    <div class="gt">⊞ grille éphémère verticale</div>
    <div id="gh-angle">Angle : 0°</div>
    <div id="gh-cell">Côté : — cm</div>
    <div class="gd" id="gh-dist">Survolez la grille…</div>
    <div class="gesc">Échap → fermer</div>
  </div>
  <div id="coords" class="hud">X:0 · Y:0 · Z:0 cm</div>
  <div id="status" class="hud">Prêt</div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
// ═══════════════════════════════════════════════════════════
// DATA
// ═══════════════════════════════════════════════════════════
const SCENE={sj};
const MODE={json.dumps(mode)};
const US=0.01; // cm→m

// ═══════════════════════════════════════════════════════════
// DOM → STREAMLIT communication
// ═══════════════════════════════════════════════════════════
function sendAction(payload){{
  const data=JSON.stringify(payload);
  const wins=[];
  try{{wins.push(window.parent);}}catch(e){{}}
  try{{wins.push(window.parent.parent);}}catch(e){{}}
  for(const w of wins){{
    try{{
      const inp=w.document.querySelector('input[placeholder="__3ds__"]');
      if(inp){{
        const setter=Object.getOwnPropertyDescriptor(w.HTMLInputElement.prototype,'value').set;
        setter.call(inp,data);
        inp.dispatchEvent(new Event('input',{{bubbles:true}}));
        return true;
      }}
    }}catch(e){{}}
  }}
  return false;
}}

// ═══════════════════════════════════════════════════════════
// RENDERER / CAMERA
// ═══════════════════════════════════════════════════════════
const wrap=document.getElementById('wrap');
const W=wrap.clientWidth,H={height};
const renderer=new THREE.WebGLRenderer({{antialias:true}});
renderer.setSize(W,H); renderer.setPixelRatio(Math.min(window.devicePixelRatio,2));
renderer.setClearColor(0xffffff,1); wrap.appendChild(renderer.domElement);
const threeScene=new THREE.Scene(); threeScene.background=new THREE.Color(0xffffff);
const camera=new THREE.PerspectiveCamera(55,W/H,0.01,5000);
camera.position.set(8,5,12); camera.lookAt(0,0,0);

// Lights
threeScene.add(new THREE.AmbientLight(0xffffff,1.0));
const dl=new THREE.DirectionalLight(0xffffff,0.3); dl.position.set(10,20,10); threeScene.add(dl);

// Orbit
let sph={{theta:0.6,phi:0.9,r:18}},tgt=new THREE.Vector3();
let isRD=false,isMD=false,lm={{x:0,y:0}};
function applyCamera(){{
  const sp=Math.sin(sph.phi),cp=Math.cos(sph.phi);
  camera.position.set(tgt.x+sph.r*sp*Math.sin(sph.theta),tgt.y+sph.r*cp,tgt.z+sph.r*sp*Math.cos(sph.theta));
  camera.lookAt(tgt);
}}
applyCamera();
const cv=renderer.domElement;
cv.addEventListener('contextmenu',e=>e.preventDefault());
cv.addEventListener('mousedown',e=>{{
  if(e.button===2)isRD=true; if(e.button===1){{isMD=true;e.preventDefault();}}
  lm={{x:e.clientX,y:e.clientY}};
}});
window.addEventListener('mouseup',()=>{{isRD=false;isMD=false;}});
window.addEventListener('mousemove',e=>{{
  const dx=e.clientX-lm.x,dy=e.clientY-lm.y; lm={{x:e.clientX,y:e.clientY}};
  if(isRD){{ sph.theta-=dx*0.005; sph.phi=Math.max(0.05,Math.min(Math.PI-0.05,sph.phi+dy*0.005)); applyCamera(); }}
  if(isMD){{
    const sp=sph.r*0.0008,right=new THREE.Vector3();
    right.crossVectors(camera.getWorldDirection(new THREE.Vector3()),camera.up).normalize();
    tgt.addScaledVector(right,-dx*sp); tgt.addScaledVector(camera.up,dy*sp); applyCamera();
  }}
  updateCoords(e);
}});
cv.addEventListener('wheel',e=>{{
  e.preventDefault(); sph.r=Math.max(0.3,Math.min(800,sph.r*(1+e.deltaY*0.001))); applyCamera();
}},{{passive:false}});

// ─── Background grid + axes ────────────────────────────────
if(SCENE.showGrid){{
  const g1=new THREE.GridHelper(200,200,0xe0e0e0,0xe0e0e0); g1.material.transparent=true; g1.material.opacity=0.6; threeScene.add(g1);
  threeScene.add(new THREE.GridHelper(200,20,0xbbbbbb,0xbbbbbb));
}}
if(SCENE.showAxes){{
  const L=3,mat=new THREE.LineBasicMaterial({{color:0x999999}});
  [[[0,0,0],[L,0,0]],[[0,0,0],[0,L,0]],[[0,0,0],[0,0,L]]].forEach(pts=>
    threeScene.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts.map(p=>new THREE.Vector3(...p))),mat)));
}}

// ═══════════════════════════════════════════════════════════
// VERTICAL EPHEMERAL GRID
// ═══════════════════════════════════════════════════════════
const VGRID={{
  active:false,
  origin:new THREE.Vector3(),
  angle:SCENE.gridAngle||0,          // degrees, Y-axis rotation
  cellSize:SCENE.gridCellSize||10,   // cm
  extent:SCENE.gridExtent||8,        // cells each side
  group:new THREE.Group(),
  // Plane axes (computed from angle)
  axisH:new THREE.Vector3(),  // horizontal axis in grid plane
  axisV:new THREE.Vector3(0,1,0),  // always world-up
  plane:new THREE.Plane(),
  // Hover indicator
  hoverMesh:null,
  hoverPos:null, // {iu,iv,distCm,worldPos}
}};
threeScene.add(VGRID.group);

function vgridUpdateAxes(){{
  const a=VGRID.angle*Math.PI/180;
  VGRID.axisH.set(Math.cos(a),0,Math.sin(a));
  // Normal = perpendicular to axisH in XZ, pointing "outward"
  const normal=new THREE.Vector3(-Math.sin(a),0,Math.cos(a));
  VGRID.plane.setFromNormalAndCoplanarPoint(normal,VGRID.origin);
}}

function buildVGrid(){{
  // Clear
  while(VGRID.group.children.length) VGRID.group.remove(VGRID.group.children[0]);
  VGRID.hoverMesh=null;
  if(!VGRID.active) return;

  vgridUpdateAxes();

  const N=VGRID.extent, S=VGRID.cellSize*US;
  const aH=VGRID.axisH, aV=VGRID.axisV, O=VGRID.origin;

  // Grid lines — lines only, no node spheres
  const matL=new THREE.LineBasicMaterial({{color:0x3a7bd5,transparent:true,opacity:0.55}});
  const matC=new THREE.LineBasicMaterial({{color:0xf59e0b,transparent:true,opacity:0.9}}); // origin cross

  // Horizontal lines (run along axisH)
  for(let j=-N;j<=N;j++){{
    const isO=(j===0);
    const start=O.clone().addScaledVector(aH,-N*S).addScaledVector(aV,j*S);
    const end  =O.clone().addScaledVector(aH, N*S).addScaledVector(aV,j*S);
    const line=new THREE.Line(new THREE.BufferGeometry().setFromPoints([start,end]),isO?matC:matL);
    VGRID.group.add(line);
  }}

  // Vertical lines (run along axisV)
  for(let i=-N;i<=N;i++){{
    const isO=(i===0);
    const start=O.clone().addScaledVector(aH,i*S).addScaledVector(aV,-N*S);
    const end  =O.clone().addScaledVector(aH,i*S).addScaledVector(aV, N*S);
    const line=new THREE.Line(new THREE.BufferGeometry().setFromPoints([start,end]),isO?matC:matL);
    VGRID.group.add(line);
  }}

  // Origin dot
  const odot=new THREE.Mesh(new THREE.SphereGeometry(0.05,8,6),
    new THREE.MeshBasicMaterial({{color:0xf59e0b}}));
  odot.position.copy(O); VGRID.group.add(odot);

  // Hover indicator (invisible until hover)
  VGRID.hoverMesh=new THREE.Mesh(new THREE.SphereGeometry(0.07,10,8),
    new THREE.MeshBasicMaterial({{color:0x3fb950,transparent:true,opacity:0.9}}));
  VGRID.hoverMesh.visible=false;
  VGRID.group.add(VGRID.hoverMesh);

  // Update HUD
  document.getElementById('ghud').style.display='block';
  document.getElementById('gh-angle').textContent='Angle : '+VGRID.angle+'°';
  document.getElementById('gh-cell').textContent='Côté : '+VGRID.cellSize+' cm';
}}

// Compute snapped grid position from a ray
function vgridSnap(ray){{
  const hit=new THREE.Vector3();
  if(!ray.ray.intersectPlane(VGRID.plane,hit)) return null;
  const diff=hit.clone().sub(VGRID.origin);
  const u=diff.dot(VGRID.axisH);
  const v=diff.y; // axisV = world up
  const S=VGRID.cellSize*US;
  const iu=Math.round(u/S), iv=Math.round(v/S);
  const snapped=VGRID.origin.clone()
    .addScaledVector(VGRID.axisH,iu*S)
    .addScaledVector(VGRID.axisV,iv*S);
  const distCm=Math.sqrt((iu*VGRID.cellSize)**2+(iv*VGRID.cellSize)**2);
  return {{worldPos:snapped,iu,iv,distCm,uCm:iu*VGRID.cellSize,vCm:iv*VGRID.cellSize}};
}}

function activateGrid(worldOrigin,angle){{
  VGRID.origin.copy(worldOrigin);
  if(angle!==undefined) VGRID.angle=angle;
  VGRID.cellSize=SCENE.gridCellSize;
  VGRID.extent=SCENE.gridExtent;
  VGRID.active=true;
  buildVGrid();
  sendAction({{type:'grid_activate',
    x:VGRID.origin.x/US,y:VGRID.origin.y/US,z:VGRID.origin.z/US,
    angle:VGRID.angle}});
}}

function dismissGrid(){{
  VGRID.active=false; buildVGrid();
  document.getElementById('ghud').style.display='none';
  VGRID.hoverPos=null;
  sendAction({{type:'grid_dismiss'}});
}}

// Restore grid from scene
if(SCENE.gridOrigin){{
  VGRID.origin.set(SCENE.gridOrigin.x*US,SCENE.gridOrigin.y*US,SCENE.gridOrigin.z*US);
  VGRID.angle=SCENE.gridAngle||0;
  VGRID.cellSize=SCENE.gridCellSize;
  VGRID.extent=SCENE.gridExtent;
  VGRID.active=true;
  buildVGrid();
}}

// ═══════════════════════════════════════════════════════════
// MATERIALS
// ═══════════════════════════════════════════════════════════
const MAT={{
  pt:    new THREE.MeshPhongMaterial({{color:0x111111,shininess:10}}),
  ptSel: new THREE.MeshPhongMaterial({{color:0xf59e0b,shininess:60,emissive:0x3d2900}}),
  ptCoin:new THREE.MeshPhongMaterial({{color:0xff3333,shininess:80,emissive:0x330000}}),
  seg:   new THREE.LineBasicMaterial({{color:0x555555}}),
  segSel:new THREE.LineBasicMaterial({{color:0x1a73e8}}),
  snap:  new THREE.MeshBasicMaterial({{color:0x1a73e8,transparent:true,opacity:0.75}}),
}};
const ptGeo=new THREE.SphereGeometry(0.06,10,8);

// ═══════════════════════════════════════════════════════════
// VIEWER SELECTION (JS-internal)
// ═══════════════════════════════════════════════════════════
let vSel={{type:null,id:null,oid:null}};

// ═══════════════════════════════════════════════════════════
// BUILD OBJECT SCENE
// ═══════════════════════════════════════════════════════════
const objGroups={{}};
function buildScene(data){{
  Object.values(objGroups).forEach(g=>threeScene.remove(g));
  Object.keys(objGroups).forEach(k=>delete objGroups[k]);
  data.objects.forEach(obj=>{{
    const g=new THREE.Group();
    g.position.set(obj.pos.x*US,obj.pos.y*US,obj.pos.z*US);
    g.quaternion.set(obj.rot.x,obj.rot.y,obj.rot.z,obj.rot.w);
    g.scale.set(obj.scl.x,obj.scl.y,obj.scl.z);
    g.userData={{type:'object',id:obj.id,name:obj.name}};
    const ptMap={{}};
    obj.points.forEach(p=>{{ptMap[p.id]=p;}});

    // Points (OD only)
    if(MODE==='object_designer'){{
      obj.points.forEach(p=>{{
        let mat=p.coin?MAT.ptCoin.clone()
              :(p.sel||(vSel.type==='point'&&vSel.id===p.id))?MAT.ptSel.clone()
              :MAT.pt.clone();
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
      const line=new THREE.Line(
        new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(pa.x*US,pa.y*US,pa.z*US),
          new THREE.Vector3(pb.x*US,pb.y*US,pb.z*US)]),
        (isSel||obj.sel)?MAT.segSel.clone():MAT.seg.clone());
      line.userData={{type:'segment',id:s.id,oid:obj.id}};
      g.add(line);
    }});

    // Coincident in PE
    if(MODE==='plan_editor'){{
      obj.points.forEach(p=>{{
        if(p.coin){{
          const m=new THREE.Mesh(new THREE.SphereGeometry(0.09,10,8),
            new THREE.MeshPhongMaterial({{color:0xff3333,transparent:true,opacity:0.7,emissive:0x220000}}));
          m.position.set(p.x*US,p.y*US,p.z*US); g.add(m);
        }}
      }});
    }}

    // Selection bbox
    if(obj.sel&&obj.points.length>0){{
      const bb=new THREE.Box3();
      obj.points.forEach(p=>bb.expandByPoint(new THREE.Vector3(p.x*US,p.y*US,p.z*US)));
      if(!bb.isEmpty()){{ bb.min.subScalar(0.1); bb.max.addScalar(0.1); g.add(new THREE.Box3Helper(bb,0x1a73e8)); }}
    }}

    // PE: proxy for picking
    if(MODE==='plan_editor'){{
      let bb=new THREE.Box3();
      if(obj.points.length>0) obj.points.forEach(p=>bb.expandByPoint(new THREE.Vector3(p.x*US,p.y*US,p.z*US)));
      else bb.set(new THREE.Vector3(-.3,-.3,-.3),new THREE.Vector3(.3,.3,.3));
      bb.min.subScalar(0.2); bb.max.addScalar(0.2);
      const sz=new THREE.Vector3(),ct=new THREE.Vector3(); bb.getSize(sz); bb.getCenter(ct);
      const proxy=new THREE.Mesh(new THREE.BoxGeometry(sz.x,sz.y,sz.z),
        new THREE.MeshBasicMaterial({{visible:false,side:THREE.DoubleSide}}));
      proxy.position.copy(ct); proxy.userData={{type:'object',id:obj.id}}; g.add(proxy);
    }}

    // Anchor sphere (PE, selected)
    if(MODE==='plan_editor'&&obj.sel){{
      const m=new THREE.Mesh(new THREE.SphereGeometry(0.08,10,8),
        new THREE.MeshBasicMaterial({{color:0x00ff88}}));
      m.position.set(obj.anchor.x*US,obj.anchor.y*US,obj.anchor.z*US); g.add(m);
    }}

    objGroups[obj.id]=g; threeScene.add(g);
  }});
}}
buildScene(SCENE);

// Snap indicator (OD)
let snapSph=null;
if(SCENE.snap&&MODE==='object_designer'){{
  snapSph=new THREE.Mesh(new THREE.SphereGeometry(0.09,10,8),MAT.snap.clone());
  snapSph.visible=false; threeScene.add(snapSph);
  const allPts=[];
  SCENE.objects.forEach(o=>o.points.forEach(p=>{{
    allPts.push({{w:new THREE.Vector3((o.pos.x+p.x)*US,(o.pos.y+p.y)*US,(o.pos.z+p.z)*US)}});
  }}));
  const gp0=new THREE.Plane(new THREE.Vector3(0,1,0),0);
  window.addEventListener('mousemove',ev=>{{
    if(!snapSph) return;
    const r=cv.getBoundingClientRect();
    const m2=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
    const ray2=new THREE.Raycaster(); ray2.setFromCamera(m2,camera);
    const h=new THREE.Vector3(); ray2.ray.intersectPlane(gp0,h);
    let near=null,minD=SCENE.snapDist*US;
    allPts.forEach(ap=>{{const d=h.distanceTo(ap.w);if(d<minD){{minD=d;near=ap;}}}});
    if(near){{snapSph.position.copy(near.w);snapSph.visible=true;}} else snapSph.visible=false;
  }});
}}

// Coordinates HUD
const gndPl=new THREE.Plane(new THREE.Vector3(0,1,0),0);
const coordDiv=document.getElementById('coords');
const statusDiv=document.getElementById('status');
function updateCoords(ev){{
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
  const ray=new THREE.Raycaster(); ray.setFromCamera(m,camera); const h=new THREE.Vector3();
  if(ray.ray.intersectPlane(gndPl,h))
    coordDiv.textContent=`X:${{(h.x/US).toFixed(1)}} · Y:${{(h.y/US).toFixed(1)}} · Z:${{(h.z/US).toFixed(1)}} cm`;
}}

// ═══════════════════════════════════════════════════════════
// HOVER — grid snap indicator
// ═══════════════════════════════════════════════════════════
const pickRay=new THREE.Raycaster();
pickRay.params.Line={{threshold:0.06}};

window.addEventListener('mousemove',ev=>{{
  if(!VGRID.active||!VGRID.hoverMesh) return;
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
  pickRay.setFromCamera(m,camera);
  const snapped=vgridSnap(pickRay);
  if(snapped){{
    VGRID.hoverMesh.position.copy(snapped.worldPos);
    VGRID.hoverMesh.visible=true;
    VGRID.hoverPos=snapped;
    document.getElementById('gh-dist').textContent=
      `Dist origine : ${{snapped.distCm.toFixed(1)}} cm  (H:${{snapped.uCm.toFixed(1)}} V:${{snapped.vCm.toFixed(1)}})`;
  }} else {{
    VGRID.hoverMesh.visible=false; VGRID.hoverPos=null;
    document.getElementById('gh-dist').textContent='Survolez la grille…';
  }}
}});

// ═══════════════════════════════════════════════════════════
// CLICK HANDLER
// ═══════════════════════════════════════════════════════════
cv.addEventListener('click',ev=>{{
  if(isRD) return;
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((ev.clientX-r.left)/W)*2-1,-((ev.clientY-r.top)/H)*2+1);
  pickRay.setFromCamera(m,camera);

  // 1) Grid hover position → create/place
  if(VGRID.active&&VGRID.hoverPos){{
    const p=VGRID.hoverPos.worldPos;
    const payload={{
      x:p.x/US,y:p.y/US,z:p.z/US,
      gridOriginX:VGRID.origin.x/US,gridOriginY:VGRID.origin.y/US,gridOriginZ:VGRID.origin.z/US,
      gridAngle:VGRID.angle,
      type:MODE==='object_designer'?'grid_click_od':'grid_click_pe',
    }};
    sendAction(payload);
    statusDiv.textContent=`Nœud (${{(p.x/US).toFixed(1)}}, ${{(p.y/US).toFixed(1)}}, ${{(p.z/US).toFixed(1)}}) → panneau ↓`;
    return;
  }}

  // 2) Pick objects / points
  const tgts=[];
  Object.values(objGroups).forEach(g=>g.traverse(c=>{{if(c.userData&&c.userData.type)tgts.push(c);}}));
  const hits=pickRay.intersectObjects(tgts,false);

  if(hits.length>0){{
    const ud=hits[0].object.userData;
    if(MODE==='plan_editor'){{
      const oid=ud.oid||ud.id;
      vSel={{type:'object',id:oid,oid}};
      sendAction({{type:'select_object',id:oid}});
      statusDiv.textContent='Objet #'+oid+' sélectionné';
    }} else {{
      // OD mode: select + activate grid at world position of the point
      vSel={{type:ud.type,id:ud.id,oid:ud.oid}};
      buildScene(SCENE);
      statusDiv.textContent=(ud.type==='point'?'Point':'Segment')+' #'+ud.id+' — Suppr=supprimer';

      // Activate vertical grid at this point's world position
      if(ud.type==='point'){{
        const obj=SCENE.objects.find(o=>o.id===ud.oid);
        if(obj){{
          const pt=obj.points.find(p=>p.id===ud.id);
          if(pt){{
            const wx=(obj.pos.x+pt.x)*US;
            const wy=(obj.pos.y+pt.y)*US;
            const wz=(obj.pos.z+pt.z)*US;
            activateGrid(new THREE.Vector3(wx,wy,wz), VGRID.angle);
            statusDiv.textContent='Point #'+ud.id+' — grille activée · Suppr=supprimer';
          }}
        }}
      }}
    }}
    return;
  }}

  // 3) Click on ground → activate grid there
  const gHit=new THREE.Vector3();
  if(pickRay.ray.intersectPlane(gndPl,gHit)){{
    activateGrid(gHit, VGRID.angle);
    statusDiv.textContent='Grille verticale activée — survolez pour snapper, cliquez pour créer';
  }}
}});

// ═══════════════════════════════════════════════════════════
// KEYBOARD
// ═══════════════════════════════════════════════════════════
window.addEventListener('keydown',ev=>{{
  if((ev.key==='Delete'||ev.key==='Backspace')&&MODE==='object_designer'){{
    if(vSel.type==='point'){{
      sendAction({{type:'delete_point',id:vSel.id,
        gridOriginX:VGRID.origin.x/US,gridOriginY:VGRID.origin.y/US,gridOriginZ:VGRID.origin.z/US,
        gridAngle:VGRID.angle}});
      statusDiv.textContent='Point #'+vSel.id+' supprimé'; vSel={{type:null,id:null,oid:null}};
    }} else if(vSel.type==='segment'){{
      sendAction({{type:'delete_segment',id:vSel.id,
        gridOriginX:VGRID.origin.x/US,gridOriginY:VGRID.origin.y/US,gridOriginZ:VGRID.origin.z/US,
        gridAngle:VGRID.angle}});
      statusDiv.textContent='Segment #'+vSel.id+' supprimé'; vSel={{type:null,id:null,oid:null}};
    }}
    ev.preventDefault();
  }}
  if(ev.key==='Escape'){{ dismissGrid(); statusDiv.textContent='Grille fermée'; }}
}});

// ═══════════════════════════════════════════════════════════
// RENDER LOOP + RESIZE
// ═══════════════════════════════════════════════════════════
(function loop(){{ requestAnimationFrame(loop); renderer.render(threeScene,camera); }})();
new ResizeObserver(()=>{{
  const nw=wrap.clientWidth; renderer.setSize(nw,{height});
  camera.aspect=nw/{height}; camera.updateProjectionMatrix();
}}).observe(wrap);
</script>
</body></html>"""

    st.markdown('<div class="viewer-wrap">', unsafe_allow_html=True)
    st_html(html_code, height=height+4, scrolling=False)
    st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRID CONTROL BAR (displayed below viewer when grid is active)
# ─────────────────────────────────────────────────────────────────────────────
def render_grid_controls():
    """Barre de contrôle grille éphémère — apparaît sous le viewer quand la grille est active."""
    go=st.session_state.get("grid_origin")
    if go is None:
        # Hint when no grid
        st.markdown('<div class="info-box" style="margin:4px 0">💡 <b>Grille éphémère</b> : '
                    'cliquez un point existant ou le sol dans la vue 3D pour activer la grille verticale.</div>',
                    unsafe_allow_html=True)
        return

    st.markdown('<div class="grid-bar">'
                '<div class="grid-bar-title">⊞ grille éphémère active</div>', unsafe_allow_html=True)

    c1,c2,c3=st.columns([3,3,1])

    # Cell size
    new_cell=c1.number_input(
        "Côté d'une cellule (cm)",
        min_value=0.5, max_value=500.0,
        value=float(st.session_state["grid_cell_size"]),
        step=0.5, format="%.1f", key="gc_cell")
    if new_cell != st.session_state["grid_cell_size"]:
        st.session_state["grid_cell_size"]=new_cell; st.rerun()

    # Angle slider — every degree
    new_angle=c2.slider(
        "Angle de la grille (°)",
        min_value=0, max_value=359,
        value=int(st.session_state["grid_angle"]),
        step=1, key="gc_angle",
        help="Orientation de la grille autour de l'axe vertical — 1° par cran")
    if new_angle != st.session_state["grid_angle"]:
        st.session_state["grid_angle"]=new_angle; st.rerun()

    c3.markdown("<br>",unsafe_allow_html=True)
    if c3.button("✕",key="close_grid_bar",help="Fermer la grille"):
        st.session_state["grid_origin"]=None; st.rerun()

    # Extent
    new_ext=st.slider(
        "Étendue (cellules de chaque côté)",
        min_value=2, max_value=20,
        value=int(st.session_state["grid_extent"]),
        step=1, key="gc_ext")
    if new_ext != st.session_state["grid_extent"]:
        st.session_state["grid_extent"]=new_ext; st.rerun()

    ox,oy,oz=go["x"],go["y"],go["z"]
    st.markdown(
        f'<div style="font-size:10px;color:#484f58;margin-top:2px">'
        f'Origine : X {ox:.1f} · Y {oy:.1f} · Z {oz:.1f} cm &nbsp;|&nbsp; '
        f'Angle : {st.session_state["grid_angle"]}° &nbsp;|&nbsp; '
        f'Côté : {st.session_state["grid_cell_size"]:.1f} cm</div>',
        unsafe_allow_html=True)
    st.markdown('</div>',unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PLAN EDITOR PANEL
# ─────────────────────────────────────────────────────────────────────────────
def _obj_idx(obj_df,oid):
    m=obj_df.index[obj_df["object_id"]==oid]; return m[0] if len(m) else None

def panel_plan_editor(obj_df,pts_df,seg_df,sel_oid,coinc_ids):

    # Pending placement
    pending=st.session_state.get("pending_place")
    if pending:
        st.markdown(
            f'<div class="pending-box">📍 <strong>Nœud grille cliqué</strong> : '
            f'X {pending["x"]:.1f} · Y {pending["y"]:.1f} · Z {pending["z"]:.1f} cm<br>'
            f'Sélectionnez un objet et cliquez "Placer ici".</div>', unsafe_allow_html=True)
        if sel_oid is not None:
            row=obj_df[obj_df["object_id"]==sel_oid]
            if not row.empty:
                o=row.iloc[0]
                ax,ay,az=float(o.get("anchor_x",0)),float(o.get("anchor_y",0)),float(o.get("anchor_z",0))
                npx,npy,npz=pending["x"]-ax,pending["y"]-ay,pending["z"]-az
                st.markdown(f'<div class="info-box">Ancre ({ax:.1f},{ay:.1f},{az:.1f}) → obj à ({npx:.1f},{npy:.1f},{npz:.1f}) cm</div>',unsafe_allow_html=True)
                if st.button("📦 Placer l'objet ici",key="do_place"):
                    idx=_obj_idx(obj_df,sel_oid)
                    if idx is not None:
                        df2=obj_df.copy(); df2.at[idx,"pos_x"]=npx; df2.at[idx,"pos_y"]=npy; df2.at[idx,"pos_z"]=npz
                        save_parquet(df2,OBJ_KEY)
                    st.session_state["pending_place"]=None; st.rerun()
        if st.button("❌ Annuler",key="cancel_place"):
            st.session_state["pending_place"]=None; st.rerun()
        st.divider()

    if coinc_ids:
        st.markdown(f'<div class="pending-box" style="background:#1f0d0d;border-color:#5a1a1a;color:#f78166">'
                    f'⚠️ <strong>{len(coinc_ids)} points coïncidents</strong> (rouge dans la vue)</div>',unsafe_allow_html=True)

    if sel_oid is None:
        st.markdown('<div class="info-box">👆 Cliquez un objet dans la vue ou dans la liste.</div>',unsafe_allow_html=True)
        return

    row=obj_df[obj_df["object_id"]==sel_oid]
    if row.empty: return
    obj=row.iloc[0]
    px,py,pz=float(obj["pos_x"]),float(obj["pos_y"]),float(obj["pos_z"])
    qx,qy,qz,qw=float(obj["rot_x"]),float(obj["rot_y"]),float(obj["rot_z"]),float(obj["rot_w"])
    ex,ey,ez=quat_to_euler(qx,qy,qz,qw)
    sx,sy,sz=float(obj["scale_x"]),float(obj["scale_y"]),float(obj["scale_z"])
    n_p=len(pts_df[pts_df["object_id"]==sel_oid]) if not pts_df.empty else 0
    n_s=len(seg_df[seg_df["object_id"]==sel_oid])  if not seg_df.empty else 0

    st.markdown(
        f'<div class="metric-row">'
        f'<div class="metric-card"><div class="metric-val">{obj["name"]}</div><div class="metric-lbl">Objet</div></div>'
        f'<div class="metric-card"><div class="metric-val">{n_p}</div><div class="metric-lbl">Points</div></div>'
        f'<div class="metric-card"><div class="metric-val">{n_s}</div><div class="metric-lbl">Segments</div></div>'
        f'</div>',unsafe_allow_html=True)
    st.markdown(
        f'<div class="pos-display">'
        f'<div class="pos-axis"><span>X</span>{px:.1f}</div>'
        f'<div class="pos-axis"><span>Y</span>{py:.1f}</div>'
        f'<div class="pos-axis"><span>Z</span>{pz:.1f}</div>'
        f'<div class="pos-axis" style="margin-left:10px"><span>RY</span>{ey:.1f}°</div>'
        f'</div>',unsafe_allow_html=True)

    tabs=st.tabs(["🕹 Déplacer","🔄 Pivoter","📐 Échelle","⚓ Ancre","📍 Exact","↗ Aligner","🗑"])

    with tabs[0]:
        step=st.number_input("Pas (cm)",min_value=0.1,max_value=9999.0,
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
        mc.markdown(f"<div style='text-align:center;padding:8px 0;border:1px solid #21262d;border-radius:5px;font-size:10px;color:#888'>X{px:.1f}<br>Z{pz:.1f}</div>",unsafe_allow_html=True)
        if r.button("▶ +X",key="m_px",use_container_width=True): _mv(dx=+step)
        _,bc,_=st.columns([1,1,1])
        if bc.button("⬇ +Z",key="m_pz",use_container_width=True): _mv(dz=+step)
        st.markdown('<p class="move-lbl">Vertical Y</p>',unsafe_allow_html=True)
        y1,y2,y3=st.columns(3)
        if y1.button("▲ +Y",key="m_py",use_container_width=True): _mv(dy=+step)
        y2.markdown(f"<div style='text-align:center;padding:5px 0;font-size:10px;color:#888'>Y{py:.1f}</div>",unsafe_allow_html=True)
        if y3.button("▼ −Y",key="m_my",use_container_width=True): _mv(dy=-step)

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
            df2.at[idx,"rot_x"]=nx; df2.at[idx,"rot_y"]=ny; df2.at[idx,"rot_z"]=nz; df2.at[idx,"rot_w"]=nw
            save_parquet(df2,OBJ_KEY); st.rerun()
        for ll,ax in [("Axe Y — horizontal","y"),("Axe X — tilt","x"),("Axe Z — roulis","z")]:
            st.markdown(f'<p class="move-lbl">{ll}</p>',unsafe_allow_html=True)
            c1,c2=st.columns(2)
            if c1.button(f"↺ −{rstep:.1f}°",key=f"r{ax}m",use_container_width=True): _rot(ax,-rstep)
            if c2.button(f"↻ +{rstep:.1f}°",key=f"r{ax}p",use_container_width=True): _rot(ax,+rstep)
        st.markdown(f'<div class="info-box">RX {ex:.1f}° RY {ey:.1f}° RZ {ez:.1f}°</div>',unsafe_allow_html=True)
        if st.button("⟲ Réinitialiser",key="rot_rst"):
            idx=_obj_idx(obj_df,sel_oid)
            if idx:
                df2=obj_df.copy(); df2.at[idx,"rot_x"]=0; df2.at[idx,"rot_y"]=0; df2.at[idx,"rot_z"]=0; df2.at[idx,"rot_w"]=1
                save_parquet(df2,OBJ_KEY); st.rerun()

    with tabs[2]:
        sstep=st.number_input("Pas",min_value=0.01,max_value=100.0,value=st.session_state["scale_step"],step=0.05,format="%.2f",key="v_ss")
        st.session_state["scale_step"]=sstep; unif=st.checkbox("Uniforme",value=True,key="scl_u")
        def _scl(ds,ax=None):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is None: return
            df2=obj_df.copy(); axes=["scale_x","scale_y","scale_z"] if unif else ([f"scale_{ax}"] if ax else ["scale_x"])
            for a in axes: df2.at[idx,a]=max(0.01,float(df2.at[idx,a])+ds)
            save_parquet(df2,OBJ_KEY); st.rerun()
        if unif:
            c1,c2=st.columns(2)
            if c1.button(f"▲ +{sstep:.2f}",key="su_p",use_container_width=True): _scl(+sstep)
            if c2.button(f"▼ −{sstep:.2f}",key="su_m",use_container_width=True): _scl(-sstep)
        else:
            for ll2,ax2 in [("X","x"),("Y","y"),("Z","z")]:
                c1,c2=st.columns(2)
                if c1.button(f"▲{ll2}",key=f"s{ax2}p",use_container_width=True): _scl(+sstep,ax2)
                if c2.button(f"▼{ll2}",key=f"s{ax2}m",use_container_width=True): _scl(-sstep,ax2)

    with tabs[3]:
        ax_=float(obj.get("anchor_x",0)); ay_=float(obj.get("anchor_y",0)); az_=float(obj.get("anchor_z",0))
        st.markdown(f'<div class="info-box">Ancre locale actuelle : ({ax_:.1f},{ay_:.1f},{az_:.1f}) cm<br>'
                    f'Point vert 🟢 dans la vue = ancre. Lors d\'un placement, l\'ancre est positionnée sur le nœud grille.</div>',unsafe_allow_html=True)
        o_pts=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
        if not o_pts.empty:
            pt_map={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":(float(r["x"]),float(r["y"]),float(r["z"])) for _,r in o_pts.iterrows()}
            pt_map["Origine (0,0,0)"]=(0.,0.,0.)
            ch=st.selectbox("Point d'ancrage",list(pt_map.keys()),key="anch_pick")
            if st.button("Définir",key="set_anch"):
                px2,py2,pz2=pt_map[ch]; idx=_obj_idx(obj_df,sel_oid)
                if idx is not None:
                    df2=obj_df.copy(); df2.at[idx,"anchor_x"]=px2; df2.at[idx,"anchor_y"]=py2; df2.at[idx,"anchor_z"]=pz2
                    save_parquet(df2,OBJ_KEY); st.success("Ancre mise à jour"); st.rerun()

    with tabs[4]:
        c1,c2,c3=st.columns(3)
        npx_=c1.number_input("X",value=px,step=1.0,format="%.1f",key=f"apx{sel_oid}")
        npy_=c2.number_input("Y",value=py,step=1.0,format="%.1f",key=f"apy{sel_oid}")
        npz_=c3.number_input("Z",value=pz,step=1.0,format="%.1f",key=f"apz{sel_oid}")
        if st.button("Appliquer position",key="abs_pos"):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is not None:
                df2=obj_df.copy(); df2.at[idx,"pos_x"]=npx_; df2.at[idx,"pos_y"]=npy_; df2.at[idx,"pos_z"]=npz_
                save_parquet(df2,OBJ_KEY); st.rerun()

    with tabs[5]:
        o_pts=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
        if o_pts.empty: st.info("Ajoutez d'abord des points.")
        else:
            pt_map2={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":(float(r["x"]),float(r["y"]),float(r["z"])) for _,r in o_pts.iterrows()}
            ref_lbl=st.selectbox("Point de référence (local)",list(pt_map2.keys()),key="aref")
            ref=pt_map2[ref_lbl]
            c1,c2,c3=st.columns(3)
            tx=c1.number_input("Cible X",value=px,step=1.0,format="%.1f",key=f"tx{sel_oid}")
            ty=c2.number_input("Cible Y",value=py,step=1.0,format="%.1f",key=f"ty{sel_oid}")
            tz=c3.number_input("Cible Z",value=pz,step=1.0,format="%.1f",key=f"tz{sel_oid}")
            pending2=st.session_state.get("pending_place")
            if pending2:
                if st.button(f"🎯 Grille ({pending2['x']:.1f},{pending2['y']:.1f},{pending2['z']:.1f})",key="grid_tgt"):
                    tx,ty,tz=pending2["x"],pending2["y"],pending2["z"]
            if st.button("↗ Aligner",key="do_align",use_container_width=True):
                idx=_obj_idx(obj_df,sel_oid)
                if idx is not None:
                    df2=obj_df.copy(); df2.at[idx,"pos_x"]=tx-ref[0]; df2.at[idx,"pos_y"]=ty-ref[1]; df2.at[idx,"pos_z"]=tz-ref[2]
                    save_parquet(df2,OBJ_KEY); st.rerun()

    with tabs[6]:
        st.warning(f"Supprimer **{obj['name']}** et tous ses points / segments ?")
        if st.button("🗑 Confirmer",key="del_obj_c"):
            for d_,k_ in [(obj_df[obj_df["object_id"]!=sel_oid],OBJ_KEY),
                          (pts_df[pts_df["object_id"]!=sel_oid] if not pts_df.empty else pts_df,PTS_KEY),
                          (seg_df[seg_df["object_id"]!=sel_oid] if not seg_df.empty else seg_df,SEG_KEY)]:
                save_parquet(d_,k_)
            st.session_state["object_id"]=None; st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# OBJECT DESIGNER PANEL
# ─────────────────────────────────────────────────────────────────────────────
def panel_object_designer(obj_df,pts_df,seg_df,sel_oid):

    pending=st.session_state.get("pending_pt")
    if pending and sel_oid is not None:
        st.markdown(
            f'<div class="pending-box">📍 <strong>Nœud grille cliqué</strong> : '
            f'X {pending["x"]:.1f} · Y {pending["y"]:.1f} · Z {pending["z"]:.1f} cm<br>'
            f'Relatif à l\'objet (pos_x={float(obj_df[obj_df["object_id"]==sel_oid].iloc[0]["pos_x"]):.1f})</div>',
            unsafe_allow_html=True)
        obj_row=obj_df[obj_df["object_id"]==sel_oid]
        if not obj_row.empty:
            o=obj_row.iloc[0]
            lx=pending["x"]-float(o["pos_x"]); ly=pending["y"]-float(o["pos_y"]); lz=pending["z"]-float(o["pos_z"])
            st.markdown(f'<div class="info-box">Coordonnées locales : ({lx:.1f},{ly:.1f},{lz:.1f}) cm</div>',unsafe_allow_html=True)
        c1,c2=st.columns(2)
        if c1.button("✅ Créer point ici",key="conf_gpt"):
            obj_row2=obj_df[obj_df["object_id"]==sel_oid]
            if not obj_row2.empty:
                o2=obj_row2.iloc[0]
                lx2=pending["x"]-float(o2["pos_x"]); ly2=pending["y"]-float(o2["pos_y"]); lz2=pending["z"]-float(o2["pos_z"])
                pid=next_id(pts_df,"point_id")
                p2=pd.concat([pts_df,pd.DataFrame([{"point_id":pid,"object_id":sel_oid,"x":lx2,"y":ly2,"z":lz2}])],ignore_index=True)
                save_parquet(p2,PTS_KEY)
            st.session_state["pending_pt"]=None; st.rerun()
        if c2.button("❌ Ignorer",key="can_gpt"):
            st.session_state["pending_pt"]=None; st.rerun()
        st.divider()

    if sel_oid is None:
        st.markdown('<div class="info-box">👆 Sélectionnez un objet.<br><br>'
                    '🖱 <b>Clic gauche sur un point existant</b> → sélectionner + activer grille verticale<br>'
                    '🖱 <b>Clic sur le sol</b> → activer grille verticale<br>'
                    '⌨ <b>Suppr</b> → supprimer le point / segment sélectionné<br>'
                    '⌨ <b>Échap</b> → fermer la grille</div>',
                    unsafe_allow_html=True); return

    if obj_df[obj_df["object_id"]==sel_oid].empty: return
    o_pts=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
    o_segs=seg_df[seg_df["object_id"]==sel_oid]  if not seg_df.empty else pd.DataFrame()

    tab_pts,tab_segs,tab_csv=st.tabs(["📍 Points","🔗 Segments","⬇ CSV"])

    with tab_pts:
        with st.expander("➕ Ajouter un point",expanded=o_pts.empty):
            c1,c2,c3,c4=st.columns([2,2,2,1])
            nx=c1.number_input("X",value=0.0,step=1.0,format="%.1f",key="np_x")
            ny=c2.number_input("Y",value=0.0,step=1.0,format="%.1f",key="np_y")
            nz=c3.number_input("Z",value=0.0,step=1.0,format="%.1f",key="np_z")
            c4.markdown("<br>",unsafe_allow_html=True)
            if c4.button("OK",key="add_pt"):
                pid=next_id(pts_df,"point_id")
                save_parquet(pd.concat([pts_df,pd.DataFrame([{"point_id":pid,"object_id":sel_oid,"x":float(nx),"y":float(ny),"z":float(nz)}])],ignore_index=True),PTS_KEY)
                st.rerun()

        if o_pts.empty: st.info("Aucun point."); return

        pt_map3={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":int(r["point_id"]) for _,r in o_pts.iterrows()}
        sel_lbl=st.selectbox("Point actif",list(pt_map3.keys()),key="sel_pt_lbl")
        sel_pid=pt_map3[sel_lbl]
        pt_row=o_pts[o_pts["point_id"]==sel_pid].iloc[0]
        cx,cy,cz=float(pt_row["x"]),float(pt_row["y"]),float(pt_row["z"])

        st.markdown(f'<div class="pos-display"><div class="pos-axis"><span>X</span>{cx:.1f}</div>'
                    f'<div class="pos-axis"><span>Y</span>{cy:.1f}</div>'
                    f'<div class="pos-axis"><span>Z</span>{cz:.1f}</div></div>',unsafe_allow_html=True)

        pstep=st.number_input("Pas (cm)",min_value=0.1,max_value=9999.0,
            value=st.session_state["pt_move_step"],step=0.1,format="%.1f",key="pt_step")
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
        m2.markdown(f"<div style='text-align:center;padding:7px 0;border:1px solid #21262d;border-radius:5px;font-size:10px;color:#888'>X{cx:.1f}<br>Z{cz:.1f}</div>",unsafe_allow_html=True)
        if r2.button("▶ +X",key="pt_px",use_container_width=True): _mpt(dx=+pstep)
        _,bc2,_=st.columns([1,1,1])
        if bc2.button("⬇ +Z",key="pt_pz",use_container_width=True): _mpt(dz=+pstep)

        st.markdown('<p class="move-lbl">Vertical Y</p>',unsafe_allow_html=True)
        y1,y2,y3=st.columns(3)
        if y1.button("▲ +Y",key="pt_py",use_container_width=True): _mpt(dy=+pstep)
        y2.markdown(f"<div style='text-align:center;padding:5px 0;font-size:10px;color:#888'>Y{cy:.1f}</div>",unsafe_allow_html=True)
        if y3.button("▼ −Y",key="pt_my",use_container_width=True): _mpt(dy=-pstep)

        st.divider()
        edit=st.data_editor(o_pts[["point_id","x","y","z"]].reset_index(drop=True),
            key=f"pts_e{sel_oid}",use_container_width=True,hide_index=True,
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
            save_parquet(df2,PTS_KEY); st.success("OK"); st.rerun()
        if c2.button("🗑 Suppr. point",key="del_pt"):
            p2=pts_df[pts_df["point_id"]!=sel_pid]
            s2=seg_df[(seg_df["point_a_id"]!=sel_pid)&(seg_df["point_b_id"]!=sel_pid)] if not seg_df.empty else seg_df
            save_parquet(p2,PTS_KEY); save_parquet(s2,SEG_KEY); st.rerun()

    with tab_segs:
        if o_pts.empty or len(o_pts)<2: st.info("≥2 points requis."); return
        pt_lbl2={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":int(r["point_id"]) for _,r in o_pts.iterrows()}
        lbls=list(pt_lbl2.keys())
        c1,c2=st.columns(2)
        sa=c1.selectbox("A",lbls,key="seg_a"); sb=c2.selectbox("B",lbls,key="seg_b",index=min(1,len(lbls)-1))
        if st.button("🔗 Créer segment",key="mk_seg"):
            pa_id,pb_id=pt_lbl2[sa],pt_lbl2[sb]
            if pa_id==pb_id: st.error("Deux points distincts requis.")
            else:
                dupe=(not seg_df.empty) and not seg_df[
                    (seg_df["object_id"]==sel_oid)&
                    (((seg_df["point_a_id"]==pa_id)&(seg_df["point_b_id"]==pb_id))|
                     ((seg_df["point_a_id"]==pb_id)&(seg_df["point_b_id"]==pa_id)))].empty
                if dupe: st.warning("Existe déjà.")
                else:
                    sid=next_id(seg_df,"segment_id")
                    save_parquet(pd.concat([seg_df,pd.DataFrame([{"segment_id":sid,"object_id":sel_oid,"point_a_id":pa_id,"point_b_id":pb_id}])],ignore_index=True),SEG_KEY); st.rerun()
        if not o_segs.empty:
            st.markdown(f"**{len(o_segs)} segment(s)**")
            st.dataframe(o_segs[["segment_id","point_a_id","point_b_id"]].reset_index(drop=True),use_container_width=True,hide_index=True)
            c1,c2=st.columns([3,1])
            dsid=c1.selectbox("Supprimer",o_segs["segment_id"].tolist(),key="dseg")
            if c2.button("🗑",key="dseg_b"): save_parquet(seg_df[seg_df["segment_id"]!=dsid],SEG_KEY); st.rerun()

    with tab_csv:
        st.markdown('<div class="info-box">Format : <code>x,y,z</code> par ligne (cm)</div>',unsafe_allow_html=True)
        up=st.file_uploader("CSV",type=["csv"],key="csv_up")
        if up:
            try:
                dfc=pd.read_csv(up,names=["x","y","z"]); st.dataframe(dfc.head(10),use_container_width=True); st.markdown(f"**{len(dfc)} points**")
                if st.button("⬇ Importer",key="do_import"):
                    base=next_id(pts_df,"point_id")
                    new=[{"point_id":base+i,"object_id":sel_oid,"x":float(rr["x"]),"y":float(rr["y"]),"z":float(rr["z"])} for i,(_,rr) in enumerate(dfc.iterrows())]
                    save_parquet(pd.concat([pts_df,pd.DataFrame(new)],ignore_index=True),PTS_KEY); st.success(f"{len(new)} points"); st.rerun()
            except Exception as e: st.error(str(e))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    init_session()

    if not st.session_state["r2_ready"]:
        try: init_r2_tables(); st.session_state["r2_ready"]=True
        except Exception as e: st.warning(f"R2 : {e}")

    proj_df=load_parquet(PROJ_KEY,PROJ_COLS)
    obj_df=load_objects()
    pts_df=load_parquet(PTS_KEY,PTS_COLS)
    seg_df=load_parquet(SEG_KEY,SEG_COLS)

    # Message bus
    viewer_msg=st.text_input("",key="_viewer_msg",placeholder="__3ds__",label_visibility="collapsed")
    if viewer_msg and viewer_msg.startswith("{") and viewer_msg!="{}":
        process_viewer_action(viewer_msg,obj_df,pts_df,seg_df)
        st.session_state["_viewer_msg"]=""

    # ── SIDEBAR ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<div class="studio-header"><div>'
                    '<div class="studio-title">🧊 3D Design Studio</div>'
                    '<div class="studio-sub">Point · Segment · Transform</div>'
                    '</div></div>',unsafe_allow_html=True)

        mode_lbl=st.radio("Mode",["📐 Plan Editor","✏️ Object Designer"],
            index=0 if st.session_state["mode"]=="plan_editor" else 1,
            horizontal=True,label_visibility="collapsed")
        st.session_state["mode"]="plan_editor" if "Plan" in mode_lbl else "object_designer"
        bcls2="badge-plan" if st.session_state["mode"]=="plan_editor" else "badge-object"
        blbl2="PLAN EDITOR" if st.session_state["mode"]=="plan_editor" else "OBJECT DESIGNER"
        st.markdown(f'<span class="badge {bcls2}">{blbl2}</span>',unsafe_allow_html=True)
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
                    doids=obj_df[obj_df["project_id"]==dpid]["object_id"].tolist(); obj_df=obj_df[obj_df["project_id"]!=dpid]
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
                    obj_df=pd.concat([obj_df,pd.DataFrame([{"object_id":oid,"project_id":cur_pid,"name":oname.strip() or f"Objet {oid}",
                        "pos_x":0.,"pos_y":0.,"pos_z":0.,"rot_x":0.,"rot_y":0.,"rot_z":0.,"rot_w":1.,
                        "scale_x":1.,"scale_y":1.,"scale_z":1.,"anchor_x":0.,"anchor_y":0.,"anchor_z":0.}])],ignore_index=True)
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

    # ── MAIN ZONE ─────────────────────────────────────────────────────
    cur_oid=st.session_state.get("object_id")
    cur_pts=st.session_state.get("selected_pts",[])
    cur_pid2=st.session_state.get("project_id")

    coinc=set()
    if st.session_state["mode"]=="plan_editor" and not obj_df.empty and not pts_df.empty:
        coinc=find_coincident_points(obj_df,pts_df)

    scene=build_scene_json(cur_pid2,obj_df,pts_df,seg_df,cur_oid,cur_pts,coinc)
    render_viewer(scene,st.session_state["mode"],height=530)

    # Grid control bar (directly under viewer, always visible when grid active)
    render_grid_controls()

    st.markdown("<hr style='border-color:#21262d;margin:6px 0'>",unsafe_allow_html=True)

    if st.session_state["mode"]=="plan_editor":
        panel_plan_editor(obj_df,pts_df,seg_df,cur_oid,coinc)
    else:
        panel_object_designer(obj_df,pts_df,seg_df,cur_oid)


if __name__=="__main__":
    main()
