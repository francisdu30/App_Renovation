"""
3D Design Studio v8
Fixes:
- Inputs cachés sans :has() (compatible Streamlit Cloud)
- Points noirs visibles, sélectionnables, mode OD fonctionnel
- Polling scène via data-attribute sur un div (plus fiable que input value)
"""

import io, json, math
from datetime import datetime
import boto3, pandas as pd
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
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;
  font-weight:600;letter-spacing:1px;text-transform:uppercase;}
.badge-plan  {background:#1a2744;color:#58a6ff;border:1px solid #1f3a72;}
.badge-object{background:#2a1a1a;color:#f78166;border:1px solid #5a2a2a;}
.section-label{font-size:9px;letter-spacing:2px;text-transform:uppercase;
  color:var(--text2);margin:6px 0 3px 0;}
.metric-row{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin:6px 0;}
.metric-card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:7px 10px;text-align:center;}
.metric-val{font-size:15px;font-weight:700;color:var(--accent);}
.metric-lbl{font-size:9px;color:var(--text2);letter-spacing:1px;text-transform:uppercase;}
.pos-display{background:var(--bg2);border:1px solid var(--border);border-radius:6px;
  padding:7px 12px;margin:5px 0;display:flex;gap:14px;align-items:center;flex-wrap:wrap;}
.pos-axis{font-size:11px;}
.pos-axis span{color:var(--text2);font-size:9px;text-transform:uppercase;margin-right:3px;}
.move-lbl{font-size:9px;color:var(--text2);letter-spacing:1.5px;text-transform:uppercase;
  margin-bottom:2px;margin-top:6px;}
.pending-box{background:#0d1f0d;border:1px solid #2a5a2a;border-radius:6px;
  padding:9px 12px;font-size:11px;color:#6ab06a;margin:6px 0;}
.pending-box strong{color:#3fb950;}
.info-box{background:var(--bg2);border:1px solid var(--border);border-radius:6px;
  padding:9px 12px;font-size:11px;color:var(--text1);margin:6px 0;}
.info-box code{background:var(--bg3);padding:1px 4px;border-radius:3px;color:#3fb950;font-size:10px;}
.success-flash{background:#0d1f0d;border:1px solid #3fb950;border-radius:5px;
  padding:5px 10px;font-size:11px;color:#3fb950;margin:4px 0;}
.viewer-wrap{border-radius:8px;overflow:hidden;border:1px solid var(--border);}

/* ── Bus inputs: hidden by class name (set via st.markdown wrapper) ── */
.bus-hidden{
  position:absolute!important;
  opacity:0!important;
  pointer-events:none!important;
  width:1px!important;
  height:1px!important;
  overflow:hidden!important;
  top:0;left:0;
}

.stButton>button{background:var(--bg2)!important;border:1px solid var(--border)!important;
  color:var(--text0)!important;font-family:'JetBrains Mono',monospace!important;
  font-size:11px!important;border-radius:5px!important;transition:all .15s!important;}
.stButton>button:hover{border-color:var(--accent)!important;color:var(--accent)!important;}
.stTabs [data-baseweb="tab"]{font-family:'JetBrains Mono',monospace;font-size:11px;}
div[data-testid="stNumberInput"] input{font-family:'JetBrains Mono',monospace;font-size:12px;
  background:var(--bg2)!important;border-color:var(--border)!important;color:var(--text0)!important;}
div[data-testid="stDataFrame"]{font-size:11px;}
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
        o=get_r2().get_object(Bucket=st.secrets["R2_BUCKET"],Key=key)
        return pd.read_parquet(io.BytesIO(o["Body"].read()))
    except: return pd.DataFrame(columns=cols)

def save_parquet(df, key):
    buf=io.BytesIO(); df.to_parquet(buf,index=False,compression="zstd"); buf.seek(0)
    get_r2().put_object(Bucket=st.secrets["R2_BUCKET"],Key=key,Body=buf.getvalue())

PROJ_KEY="projects.parquet"; OBJ_KEY="objects.parquet"
PTS_KEY ="points.parquet";   SEG_KEY ="segments.parquet"
PROJ_COLS=["project_id","name","created_at"]
OBJ_COLS_BASE=["object_id","project_id","name","pos_x","pos_y","pos_z",
               "rot_x","rot_y","rot_z","rot_w","scale_x","scale_y","scale_z"]
OBJ_COLS_EXT={"anchor_x":0.0,"anchor_y":0.0,"anchor_z":0.0,
              "grid_cell_size":10.0,"grid_extent":8,"grid_angle":0}
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
    _ss("mode","object_designer")  # default to OD so points work immediately
    _ss("project_id",None); _ss("object_id",None)
    _ss("selected_pts",[]); _ss("show_grid",True); _ss("show_axes",True)
    _ss("snap",True); _ss("snap_dist",5.0); _ss("r2_ready",False)
    _ss("move_step",1.0); _ss("rot_step",5.0); _ss("scale_step",0.1); _ss("pt_move_step",1.0)
    _ss("grid_cell_size",10.0); _ss("grid_extent",8); _ss("grid_angle",0)
    _ss("grid_origin",None); _ss("pending_place",None)
    _ss("_prev_oid",None); _ss("_last_pt_msg","")

# ─────────────────────────────────────────────────────────────────────────────
# GRID CONFIG ↔ OBJECT
# ─────────────────────────────────────────────────────────────────────────────
def sync_grid_from_object(obj_df, oid):
    rows=obj_df[obj_df["object_id"]==oid]
    if rows.empty: return
    o=rows.iloc[0]
    st.session_state["grid_cell_size"]=float(o.get("grid_cell_size",10.0))
    st.session_state["grid_extent"]=int(o.get("grid_extent",8))
    st.session_state["grid_angle"]=int(o.get("grid_angle",0))

def save_grid_to_object(obj_df, oid, cell, extent, angle):
    rows=obj_df[obj_df["object_id"]==oid]
    if rows.empty: return obj_df
    idx=rows.index[0]; o=rows.iloc[0]
    if (float(o.get("grid_cell_size",10))==cell and
        int(o.get("grid_extent",8))==extent and
        int(o.get("grid_angle",0))==angle): return obj_df
    df2=obj_df.copy()
    df2.at[idx,"grid_cell_size"]=cell; df2.at[idx,"grid_extent"]=extent; df2.at[idx,"grid_angle"]=angle
    save_parquet(df2,OBJ_KEY); return df2

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
    cy,sy=math.cos(rz/2),math.sin(rz/2); cp,sp=math.cos(ry/2),math.sin(ry/2); cr,sr=math.cos(rx/2),math.sin(rx/2)
    return (sr*cp*cy-cr*sp*sy,cr*sp*cy+sr*cp*sy,cr*cp*sy-sr*sp*cy,cr*cp*cy+sr*sp*sy)

def compose_rot(qx,qy,qz,qw,axis,deg):
    a=math.radians(deg)/2; c,s=math.cos(a),math.sin(a)
    dq={"x":(s,0,0,c),"y":(0,s,0,c),"z":(0,0,s,c)}[axis]; dx,dy,dz,dw=dq
    return (dw*qx+dx*qw+dy*qz-dz*qy,dw*qy-dx*qz+dy*qw+dz*qx,
            dw*qz+dx*qy-dy*qx+dz*qw,dw*qw-dx*qx-dy*qy-dz*qz)

def find_coincident(obj_df,pts_df,thr=0.5):
    if pts_df.empty or obj_df.empty or len(pts_df)<2: return set()
    world=[]
    for _,pt in pts_df.iterrows():
        oid=int(pt["object_id"]); rows=obj_df[obj_df["object_id"]==oid]
        if rows.empty: continue
        o=rows.iloc[0]
        world.append((int(pt["point_id"]),oid,
                      float(pt["x"])+float(o["pos_x"]),float(pt["y"])+float(o["pos_y"]),float(pt["z"])+float(o["pos_z"])))
    coinc=set(); t2=thr**2
    for i in range(len(world)):
        for j in range(i+1,len(world)):
            if world[i][1]==world[j][1]: continue
            dx=world[i][2]-world[j][2]; dy=world[i][3]-world[j][3]; dz=world[i][4]-world[j][4]
            if dx*dx+dy*dy+dz*dz<t2: coinc.add(world[i][0]); coinc.add(world[j][0])
    return coinc

# ─────────────────────────────────────────────────────────────────────────────
# VIEWER ACTION PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────
def process_viewer_action(raw, obj_df, pts_df, seg_df):
    try: action=json.loads(raw)
    except: return
    t=action.get("type","")
    # Restore grid state
    if "gridOriginX" in action:
        st.session_state["grid_origin"]={"x":action["gridOriginX"],"y":action["gridOriginY"],"z":action["gridOriginZ"]}
    if "gridAngle"    in action: st.session_state["grid_angle"]   =int(round(float(action["gridAngle"])))%360
    if "gridCellSize" in action: st.session_state["grid_cell_size"]=float(action["gridCellSize"])
    if "gridExtent"   in action: st.session_state["grid_extent"]  =int(action["gridExtent"])

    if t=="grid_activate":
        st.session_state["grid_origin"]={"x":action["x"],"y":action["y"],"z":action["z"]}
    elif t=="grid_dismiss":
        st.session_state["grid_origin"]=None

    elif t=="grid_click_od":
        # Create point immediately in OD mode
        sel_oid=st.session_state.get("object_id")
        if sel_oid is not None:
            rows=obj_df[obj_df["object_id"]==sel_oid]
            if not rows.empty:
                o=rows.iloc[0]
                lx=round(float(action["x"])-float(o["pos_x"]),2)
                ly=round(float(action["y"])-float(o["pos_y"]),2)
                lz=round(float(action["z"])-float(o["pos_z"]),2)
                pid=next_id(pts_df,"point_id")
                pts2=pd.concat([pts_df,pd.DataFrame([{"point_id":pid,"object_id":sel_oid,"x":lx,"y":ly,"z":lz}])],ignore_index=True)
                save_parquet(pts2,PTS_KEY)
                save_grid_to_object(obj_df,sel_oid,
                    float(action.get("gridCellSize",st.session_state["grid_cell_size"])),
                    int(action.get("gridExtent",st.session_state["grid_extent"])),
                    int(action.get("gridAngle",st.session_state["grid_angle"])))
                st.session_state["_last_pt_msg"]=f"#{pid} ({lx:.1f}, {ly:.1f}, {lz:.1f}) cm"
        st.session_state["_viewer_msg"]="{}"; st.rerun()

    elif t=="grid_click_pe":
        st.session_state["pending_place"]={"x":action["x"],"y":action["y"],"z":action["z"]}

    elif t=="delete_point":
        pid=int(action["id"])
        p2=pts_df[pts_df["point_id"]!=pid]
        s2=seg_df[(seg_df["point_a_id"]!=pid)&(seg_df["point_b_id"]!=pid)] if not seg_df.empty else seg_df
        save_parquet(p2,PTS_KEY); save_parquet(s2,SEG_KEY)
        st.session_state["_viewer_msg"]="{}"; st.rerun()

    elif t=="delete_segment":
        save_parquet(seg_df[seg_df["segment_id"]!=int(action["id"])],SEG_KEY)
        st.session_state["_viewer_msg"]="{}"; st.rerun()

    elif t=="select_object":
        st.session_state["object_id"]=int(action["id"])
        st.session_state["_viewer_msg"]="{}"; st.rerun()

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
        "gridOrigin":go,"gridAngle":int(st.session_state["grid_angle"]),
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
# VIEWER — stable HTML that polls scene from a Streamlit text_area
# The viewer is NEVER recreated between reruns (stable HTML string)
# ─────────────────────────────────────────────────────────────────────────────
_VIEWER_HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{background:#fff;overflow:hidden;font-family:'JetBrains Mono',monospace;user-select:none;}
#wrap{width:100%;height:100vh;position:relative;}
/* HUDs */
.hud{position:absolute;pointer-events:none;font-size:10px;z-index:5;}
#badge{top:10px;left:10px;padding:4px 10px;border-radius:4px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;}
.badge-plan  {background:rgba(26,39,68,.92);color:#58a6ff;border:1px solid #1f3a72;}
.badge-object{background:rgba(42,26,26,.92);color:#f78166;border:1px solid #5a2a2a;}
#coords{bottom:10px;left:10px;color:#333;background:rgba(255,255,255,.92);padding:5px 10px;border-radius:4px;border:1px solid #ccc;font-size:11px;}
#status{bottom:10px;right:10px;color:#444;background:rgba(255,255,255,.92);padding:5px 10px;border-radius:4px;border:1px solid #ccc;}
#ptcount{bottom:44px;left:10px;color:#1a73e8;background:rgba(255,255,255,.92);padding:3px 8px;border-radius:4px;border:1px solid #c8d8f8;font-size:11px;font-weight:600;}
#help{top:10px;right:10px;color:#555;background:rgba(255,255,255,.92);padding:7px 10px;border-radius:6px;border:1px solid #ccc;line-height:1.8;font-size:10px;}
#pt-flash{bottom:72px;left:10px;background:rgba(10,40,10,.92);color:#3fb950;border:1px solid #3fb950;border-radius:5px;padding:5px 10px;font-size:11px;display:none;}
/* Grid info */
#ghud{top:56px;left:10px;background:rgba(10,18,40,.92);color:#58a6ff;border:1px solid #1f3a72;border-radius:6px;padding:8px 12px;display:none;font-size:11px;line-height:1.7;min-width:150px;}
#ghud .gt{font-size:9px;color:#8b949e;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:3px;}
#ghud .gd{color:#3fb950;font-weight:600;}
/* Grid panel */
#grid-panel{position:absolute;right:10px;top:56px;width:220px;background:rgba(8,14,32,.97);border:1.5px solid #1f3a72;border-radius:10px;padding:14px;color:#e6edf3;display:none;z-index:20;pointer-events:auto;box-shadow:0 4px 24px rgba(0,0,0,.4);}
#grid-panel .gp-title{font-size:9px;color:#484f58;letter-spacing:2px;text-transform:uppercase;margin-bottom:8px;}
#gp-angle-val{font-size:44px;font-weight:800;color:#58a6ff;text-align:center;line-height:1;margin-bottom:2px;letter-spacing:-2px;}
#gp-angle-sub{font-size:9px;color:#484f58;text-align:center;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px;}
#gp-angle-range{-webkit-appearance:none;width:100%;height:6px;border-radius:3px;outline:none;cursor:pointer;background:linear-gradient(to right,#1a73e8 0%,#21262d 0%);}
#gp-angle-range::-webkit-slider-thumb{-webkit-appearance:none;width:20px;height:20px;border-radius:50%;background:#58a6ff;border:2px solid #fff;cursor:pointer;}
#gp-angle-range::-moz-range-thumb{width:18px;height:18px;border-radius:50%;background:#58a6ff;border:2px solid #fff;cursor:pointer;}
#gp-presets{display:flex;flex-wrap:wrap;gap:3px;margin:8px 0;}
.gp-preset{background:#0f1f40;border:1px solid #1f3a72;color:#58a6ff;font-size:9px;padding:3px 6px;border-radius:3px;cursor:pointer;font-family:'JetBrains Mono',monospace;transition:all .1s;}
.gp-preset:hover,.gp-preset.active{background:#1a73e8;color:#fff;border-color:#1a73e8;}
.gp-label{font-size:9px;color:#8b949e;letter-spacing:1px;text-transform:uppercase;margin-top:10px;margin-bottom:3px;}
.gp-num{width:100%;background:#0f1117;border:1px solid #21262d;color:#e6edf3;font-family:'JetBrains Mono',monospace;font-size:13px;padding:5px 8px;border-radius:5px;outline:none;}
.gp-num:focus{border-color:#1a73e8;}
.gp-info{font-size:9px;color:#484f58;margin-top:8px;line-height:1.6;}
#gp-close{width:100%;margin-top:10px;padding:6px;background:#1a1a2e;border:1px solid #21262d;color:#8b949e;font-family:'JetBrains Mono',monospace;font-size:10px;border-radius:5px;cursor:pointer;}
#gp-close:hover{border-color:#f78166;color:#f78166;}
</style>
</head>
<body>
<div id="wrap">
  <div id="badge" class="hud badge-object">OBJECT DESIGNER</div>
  <div id="help" class="hud">
    Clic droit+glisser → rotation<br>
    Molette+glisser → pan · Molette → zoom<br>
    <b>Clic point</b> → sélect + grille<br>
    <b>Survol grille + clic</b> → créer point<br>
    Suppr → supprimer · Échap → fermer grille
  </div>
  <div id="ghud" class="hud">
    <div class="gt">⊞ grille</div>
    <div id="gh-ang">0°</div>
    <div class="gd" id="gh-dist">Survolez…</div>
  </div>
  <div id="ptcount" class="hud">0 pts</div>
  <div id="coords" class="hud">X:0 · Y:0 · Z:0 cm</div>
  <div id="status" class="hud">Chargement…</div>
  <div id="pt-flash" class="hud">✓</div>
  <div id="grid-panel">
    <div class="gp-title">⊞ GRILLE ÉPHÉMÈRE VERTICALE</div>
    <div id="gp-angle-val">0°</div>
    <div id="gp-angle-sub">ANGLE — 1 cran = 1°</div>
    <input type="range" id="gp-angle-range" min="0" max="359" step="1" value="0">
    <div id="gp-presets"></div>
    <div class="gp-label">Côté (cm)</div>
    <input type="number" class="gp-num" id="gp-cell" min="0.1" max="500" step="0.1" value="10">
    <div class="gp-label">Carrés de chaque côté</div>
    <input type="number" class="gp-num" id="gp-ext" min="1" max="50" step="1" value="8">
    <div class="gp-info" id="gp-info">—</div>
    <button id="gp-close">✕ Fermer (Échap)</button>
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script>
// ══════════════════════════════════════════════════
// COMMUNICATION: Viewer → Streamlit (action bus)
// Sends JSON to a hidden textarea in the parent
// ══════════════════════════════════════════════════
function sendAction(payload) {
  const data = JSON.stringify(payload);
  // Walk up frame hierarchy
  const wins = [];
  try { wins.push(window.parent); } catch(e) {}
  try { if (window.parent !== window.parent.parent) wins.push(window.parent.parent); } catch(e) {}
  for (const w of wins) {
    try {
      // Look for textarea with data-bus="action"
      const ta = w.document.querySelector('textarea[data-bus="action"]');
      if (ta) {
        ta.value = data;
        ta.dispatchEvent(new Event('input', {bubbles: true}));
        return;
      }
      // Fallback: text_input with specific label
      const inp = w.document.querySelector('input[aria-label="__action_bus__"]');
      if (inp) {
        const setter = Object.getOwnPropertyDescriptor(w.HTMLInputElement.prototype, 'value').set;
        setter.call(inp, data);
        inp.dispatchEvent(new Event('input', {bubbles: true}));
        return;
      }
    } catch(e) {}
  }
}

// ══════════════════════════════════════════════════
// COMMUNICATION: Streamlit → Viewer (scene polling)
// Reads JSON from a div[data-scene] in the parent
// ══════════════════════════════════════════════════
function getScene() {
  const wins = [];
  try { wins.push(window.parent); } catch(e) {}
  try { if (window.parent !== window.parent.parent) wins.push(window.parent.parent); } catch(e) {}
  for (const w of wins) {
    try {
      const el = w.document.querySelector('[data-scene-json]');
      if (el) {
        const v = el.getAttribute('data-scene-json');
        if (v && v.startsWith('{')) return JSON.parse(v);
      }
    } catch(e) {}
  }
  return null;
}

// ══════════════════════════════════════════════════
// RENDERER
// ══════════════════════════════════════════════════
const wrap = document.getElementById('wrap');
function getW() { return wrap.clientWidth || 800; }
function getH() { return wrap.clientHeight || 560; }

const renderer = new THREE.WebGLRenderer({antialias: true});
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setClearColor(0xffffff, 1);
wrap.appendChild(renderer.domElement);

const threeScene = new THREE.Scene();
threeScene.background = new THREE.Color(0xffffff);
const camera = new THREE.PerspectiveCamera(55, getW()/getH(), 0.01, 5000);
camera.position.set(8, 5, 12); camera.lookAt(0, 0, 0);
threeScene.add(new THREE.AmbientLight(0xffffff, 1.0));
const dl = new THREE.DirectionalLight(0xffffff, 0.4);
dl.position.set(10, 20, 10); threeScene.add(dl);

function resizeRenderer() {
  const w = getW(), h = getH();
  renderer.setSize(w, h);
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
}
resizeRenderer();
new ResizeObserver(resizeRenderer).observe(wrap);

// ── Orbit controls ─────────────────────────────────
let sph = {theta: 0.6, phi: 0.9, r: 18}, tgt = new THREE.Vector3();
let isRD = false, isMD = false, lm = {x:0, y:0};
function applyCamera() {
  camera.position.set(
    tgt.x + sph.r * Math.sin(sph.phi) * Math.sin(sph.theta),
    tgt.y + sph.r * Math.cos(sph.phi),
    tgt.z + sph.r * Math.sin(sph.phi) * Math.cos(sph.theta));
  camera.lookAt(tgt);
}
applyCamera();

const cv = renderer.domElement;
cv.addEventListener('contextmenu', e => e.preventDefault());
cv.addEventListener('mousedown', e => {
  if (e.button===2) isRD=true;
  if (e.button===1) { isMD=true; e.preventDefault(); }
  lm = {x: e.clientX, y: e.clientY};
});
window.addEventListener('mouseup', () => { isRD=false; isMD=false; });
window.addEventListener('mousemove', e => {
  const dx=e.clientX-lm.x, dy=e.clientY-lm.y;
  lm={x:e.clientX,y:e.clientY};
  if (isRD) {
    sph.theta -= dx*0.005;
    sph.phi = Math.max(0.05, Math.min(Math.PI-0.05, sph.phi+dy*0.005));
    applyCamera();
  }
  if (isMD) {
    const sp=sph.r*0.0008, right=new THREE.Vector3();
    right.crossVectors(camera.getWorldDirection(new THREE.Vector3()), camera.up).normalize();
    tgt.addScaledVector(right, -dx*sp);
    tgt.addScaledVector(camera.up, dy*sp);
    applyCamera();
  }
  onMouseMove(e);
});
cv.addEventListener('wheel', e => {
  e.preventDefault();
  sph.r = Math.max(0.3, Math.min(800, sph.r*(1+e.deltaY*0.001)));
  applyCamera();
}, {passive: false});

// ── Static scene layers ────────────────────────────
const bgGroup = new THREE.Group(); threeScene.add(bgGroup);
let lastShowGrid=null, lastShowAxes=null;
function rebuildBg(showGrid, showAxes) {
  if (showGrid===lastShowGrid && showAxes===lastShowAxes) return;
  lastShowGrid=showGrid; lastShowAxes=showAxes;
  while (bgGroup.children.length) bgGroup.remove(bgGroup.children[0]);
  if (showGrid) {
    const g1=new THREE.GridHelper(200,200,0xe0e0e0,0xe0e0e0);
    g1.material.transparent=true; g1.material.opacity=0.6; bgGroup.add(g1);
    bgGroup.add(new THREE.GridHelper(200,20,0xbbbbbb,0xbbbbbb));
  }
  if (showAxes) {
    const L=3, mat=new THREE.LineBasicMaterial({color:0x999999});
    [[[0,0,0],[L,0,0]],[[0,0,0],[0,L,0]],[[0,0,0],[0,0,L]]].forEach(pts =>
      bgGroup.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts.map(p=>new THREE.Vector3(...p))),mat)));
  }
}

// ══════════════════════════════════════════════════
// EPHEMERAL VERTICAL GRID
// ══════════════════════════════════════════════════
const VGRID = {
  active: false, origin: new THREE.Vector3(),
  angle: 0, cellSize: 10, extent: 8,
  group: new THREE.Group(),
  axisH: new THREE.Vector3(), axisV: new THREE.Vector3(0,1,0),
  plane: new THREE.Plane(), hoverMesh: null, hoverPos: null,
};
threeScene.add(VGRID.group);

function vgridUpdateAxes() {
  const a = VGRID.angle * Math.PI/180;
  VGRID.axisH.set(Math.cos(a), 0, Math.sin(a));
  VGRID.plane.setFromNormalAndCoplanarPoint(new THREE.Vector3(-Math.sin(a),0,Math.cos(a)), VGRID.origin);
}

function buildVGrid() {
  while (VGRID.group.children.length) VGRID.group.remove(VGRID.group.children[0]);
  VGRID.hoverMesh = null;
  if (!VGRID.active) return;
  vgridUpdateAxes();
  const N=VGRID.extent, S=VGRID.cellSize*0.01;
  const aH=VGRID.axisH, aV=VGRID.axisV, O=VGRID.origin;
  const matL = new THREE.LineBasicMaterial({color:0x3a7bd5, transparent:true, opacity:0.6});
  const matO = new THREE.LineBasicMaterial({color:0xf59e0b});
  for (let j=-N; j<=N; j++) {
    const s=O.clone().addScaledVector(aH,-N*S).addScaledVector(aV,j*S);
    const e=O.clone().addScaledVector(aH, N*S).addScaledVector(aV,j*S);
    VGRID.group.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([s,e]), j===0?matO:matL));
  }
  for (let i=-N; i<=N; i++) {
    const s=O.clone().addScaledVector(aH,i*S).addScaledVector(aV,-N*S);
    const e=O.clone().addScaledVector(aH,i*S).addScaledVector(aV, N*S);
    VGRID.group.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints([s,e]), i===0?matO:matL));
  }
  // Origin dot
  const odot = new THREE.Mesh(new THREE.SphereGeometry(0.06,8,6), new THREE.MeshBasicMaterial({color:0xf59e0b}));
  odot.position.copy(O); VGRID.group.add(odot);
  // Hover indicator (green dot)
  VGRID.hoverMesh = new THREE.Mesh(
    new THREE.SphereGeometry(0.09,10,8),
    new THREE.MeshBasicMaterial({color:0x3fb950, transparent:true, opacity:0.9}));
  VGRID.hoverMesh.visible = false;
  VGRID.group.add(VGRID.hoverMesh);
  updateGridPanel();
}

function vgridSnap(ray) {
  const hit = new THREE.Vector3();
  if (!ray.ray.intersectPlane(VGRID.plane, hit)) return null;
  const diff = hit.clone().sub(VGRID.origin);
  const u = diff.dot(VGRID.axisH), v = diff.y;
  const S = VGRID.cellSize * 0.01;
  const iu = Math.round(u/S), iv = Math.round(v/S);
  const snapped = VGRID.origin.clone().addScaledVector(VGRID.axisH,iu*S).addScaledVector(VGRID.axisV,iv*S);
  return {
    worldPos: snapped, iu, iv,
    distCm: Math.sqrt((iu*VGRID.cellSize)**2 + (iv*VGRID.cellSize)**2),
    uCm: iu*VGRID.cellSize, vCm: iv*VGRID.cellSize,
  };
}

function activateGrid(worldOrigin) {
  VGRID.origin.copy(worldOrigin);
  VGRID.active = true;
  buildVGrid();
  document.getElementById('grid-panel').style.display = 'block';
  document.getElementById('ghud').style.display = 'block';
  sendAction({type:'grid_activate',
    x:VGRID.origin.x/0.01, y:VGRID.origin.y/0.01, z:VGRID.origin.z/0.01,
    angle:VGRID.angle, gridCellSize:VGRID.cellSize, gridExtent:VGRID.extent});
}

function dismissGrid() {
  VGRID.active = false; buildVGrid();
  document.getElementById('grid-panel').style.display = 'none';
  document.getElementById('ghud').style.display = 'none';
  VGRID.hoverPos = null;
  sendAction({type:'grid_dismiss'});
}

// ── Grid panel controls ────────────────────────────
const PRESETS = [0,15,30,45,60,90,120,135,150,180,225,270,315];
const gpAngle = document.getElementById('gp-angle-range');
const gpAngleVal = document.getElementById('gp-angle-val');
const gpCell = document.getElementById('gp-cell');
const gpExt = document.getElementById('gp-ext');

function updateAngleDisplay(a) {
  gpAngleVal.textContent = a + '°';
  document.getElementById('gh-ang').textContent = a + '°';
  const pct = (a/359)*100;
  gpAngle.style.background = `linear-gradient(to right,#1a73e8 ${pct}%,#21262d ${pct}%)`;
  document.querySelectorAll('.gp-preset').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.angle)===a);
  });
}

function updateGridPanel() {
  gpAngle.value=VGRID.angle; gpCell.value=VGRID.cellSize; gpExt.value=VGRID.extent;
  updateAngleDisplay(VGRID.angle);
  document.getElementById('gp-info').textContent = `${VGRID.extent*2}×${VGRID.extent*2} carrés · ${VGRID.cellSize.toFixed(1)} cm`;
}

PRESETS.forEach(p => {
  const btn = document.createElement('button');
  btn.className='gp-preset'; btn.dataset.angle=p; btn.textContent=p+'°';
  btn.addEventListener('click', e => { e.stopPropagation(); VGRID.angle=p; buildVGrid(); });
  document.getElementById('gp-presets').appendChild(btn);
});

gpAngle.addEventListener('input', () => { VGRID.angle=parseInt(gpAngle.value); updateAngleDisplay(VGRID.angle); buildVGrid(); });
gpCell.addEventListener('input',  () => { const v=parseFloat(gpCell.value); if(v>0){VGRID.cellSize=Math.round(v*10)/10; buildVGrid();} });
gpExt.addEventListener('change',  () => { const v=parseInt(gpExt.value);   if(v>0){VGRID.extent=v; buildVGrid();} });
document.getElementById('gp-close').addEventListener('click', e => { e.stopPropagation(); dismissGrid(); });
document.getElementById('grid-panel').addEventListener('click',     e => e.stopPropagation());
document.getElementById('grid-panel').addEventListener('mousedown', e => e.stopPropagation());

// ══════════════════════════════════════════════════
// OBJECT SCENE
// ══════════════════════════════════════════════════
const ptGeo   = new THREE.SphereGeometry(0.10, 12, 10);  // black sphere, clearly visible
const ptGeoSel= new THREE.SphereGeometry(0.12, 12, 10);

function mkMat(color, emissive) {
  return new THREE.MeshLambertMaterial({color, emissive: emissive||0x000000});
}

let vSel = {type:null, id:null, oid:null};
let currentScene = null;
const objGroups = {};

function buildScene(data) {
  if (!data) return;
  currentScene = data;

  // Update badge
  const isOD = (data.mode==='object_designer');
  const badge = document.getElementById('badge');
  badge.className = 'hud ' + (isOD ? 'badge-object' : 'badge-plan');
  badge.textContent = isOD ? 'OBJECT DESIGNER' : 'PLAN EDITOR';

  // Remove old groups
  Object.values(objGroups).forEach(g => threeScene.remove(g));
  Object.keys(objGroups).forEach(k => delete objGroups[k]);

  rebuildBg(data.showGrid, data.showAxes);

  let totalPts = 0;

  data.objects.forEach(obj => {
    const g = new THREE.Group();
    g.position.set(obj.pos.x*0.01, obj.pos.y*0.01, obj.pos.z*0.01);
    g.quaternion.set(obj.rot.x, obj.rot.y, obj.rot.z, obj.rot.w);
    g.scale.set(obj.scl.x, obj.scl.y, obj.scl.z);
    g.userData = {type:'object', id:obj.id, name:obj.name};

    const ptMap = {};
    obj.points.forEach(p => { ptMap[p.id] = p; });
    totalPts += obj.points.length;

    // ── POINTS — always render in OD ──────────────────────────
    if (data.mode === 'object_designer') {
      obj.points.forEach(p => {
        const isSel = (vSel.type==='point' && vSel.id===p.id);
        let mat;
        if (p.coin)      mat = mkMat(0xff2222);
        else if (isSel)  mat = mkMat(0xf59e0b, 0x332200);
        else             mat = mkMat(0x111111);
        const mesh = new THREE.Mesh(isSel ? ptGeoSel : ptGeo, mat);
        mesh.position.set(p.x*0.01, p.y*0.01, p.z*0.01);
        mesh.userData = {type:'point', id:p.id, oid:obj.id};
        g.add(mesh);
      });
    }

    // ── SEGMENTS ─────────────────────────────────────────────
    obj.segments.forEach(s => {
      const pa=ptMap[s.a], pb=ptMap[s.b];
      if (!pa || !pb) return;
      const isSel = (vSel.type==='segment' && vSel.id===s.id);
      const line = new THREE.Line(
        new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(pa.x*0.01, pa.y*0.01, pa.z*0.01),
          new THREE.Vector3(pb.x*0.01, pb.y*0.01, pb.z*0.01)]),
        new THREE.LineBasicMaterial({color: (isSel||obj.sel) ? 0x1a73e8 : 0x333333}));
      line.userData = {type:'segment', id:s.id, oid:obj.id};
      g.add(line);
    });

    // ── COINCIDENT (PE) ───────────────────────────────────────
    if (data.mode==='plan_editor') {
      obj.points.forEach(p => {
        if (p.coin) {
          const m = new THREE.Mesh(new THREE.SphereGeometry(0.12,10,8), mkMat(0xff2222));
          m.position.set(p.x*0.01, p.y*0.01, p.z*0.01); g.add(m);
        }
      });
    }

    // ── SELECTION BBOX ────────────────────────────────────────
    if (obj.sel && obj.points.length > 0) {
      const bb = new THREE.Box3();
      obj.points.forEach(p => bb.expandByPoint(new THREE.Vector3(p.x*0.01, p.y*0.01, p.z*0.01)));
      if (!bb.isEmpty()) {
        bb.min.subScalar(0.15); bb.max.addScalar(0.15);
        g.add(new THREE.Box3Helper(bb, 0x1a73e8));
      }
    }

    // ── PE PROXY ─────────────────────────────────────────────
    if (data.mode === 'plan_editor') {
      let bb = new THREE.Box3();
      if (obj.points.length>0) obj.points.forEach(p=>bb.expandByPoint(new THREE.Vector3(p.x*0.01,p.y*0.01,p.z*0.01)));
      else bb.set(new THREE.Vector3(-.3,-.3,-.3),new THREE.Vector3(.3,.3,.3));
      bb.min.subScalar(0.2); bb.max.addScalar(0.2);
      const sz=new THREE.Vector3(), ct=new THREE.Vector3(); bb.getSize(sz); bb.getCenter(ct);
      const proxy=new THREE.Mesh(new THREE.BoxGeometry(sz.x,sz.y,sz.z),
        new THREE.MeshBasicMaterial({visible:false, side:THREE.DoubleSide}));
      proxy.position.copy(ct); proxy.userData={type:'object',id:obj.id}; g.add(proxy);
    }

    // ── ANCHOR (PE+selected) ──────────────────────────────────
    if (data.mode==='plan_editor' && obj.sel) {
      const m=new THREE.Mesh(new THREE.SphereGeometry(0.1,10,8),new THREE.MeshBasicMaterial({color:0x00ff88}));
      m.position.set(obj.anchor.x*0.01, obj.anchor.y*0.01, obj.anchor.z*0.01); g.add(m);
    }

    objGroups[obj.id] = g;
    threeScene.add(g);
  });

  document.getElementById('ptcount').textContent = totalPts + ' pt' + (totalPts!==1?'s':'');

  // Sync grid from scene (only if VGRID not already active to avoid losing position)
  if (data.gridOrigin && !VGRID.active) {
    VGRID.origin.set(data.gridOrigin.x*0.01, data.gridOrigin.y*0.01, data.gridOrigin.z*0.01);
    VGRID.angle    = data.gridAngle    || 0;
    VGRID.cellSize = data.gridCellSize || 10;
    VGRID.extent   = data.gridExtent   || 8;
    VGRID.active = true;
    buildVGrid();
    document.getElementById('grid-panel').style.display = 'block';
    document.getElementById('ghud').style.display = 'block';
  } else if (!data.gridOrigin && VGRID.active) {
    VGRID.active = false; buildVGrid();
    document.getElementById('grid-panel').style.display = 'none';
    document.getElementById('ghud').style.display = 'none';
  }
}

// ── Raycaster ──────────────────────────────────────
const gndPl = new THREE.Plane(new THREE.Vector3(0,1,0), 0);
const pickRay = new THREE.Raycaster();
pickRay.params.Line = {threshold: 0.08};

function onMouseMove(ev) {
  const r=cv.getBoundingClientRect();
  const m=new THREE.Vector2(((ev.clientX-r.left)/getW())*2-1, -((ev.clientY-r.top)/getH())*2+1);
  pickRay.setFromCamera(m, camera);
  // Coords
  const h=new THREE.Vector3();
  if (pickRay.ray.intersectPlane(gndPl,h))
    document.getElementById('coords').textContent = `X:${(h.x/0.01).toFixed(1)} · Y:${(h.y/0.01).toFixed(1)} · Z:${(h.z/0.01).toFixed(1)} cm`;
  // Grid hover
  if (VGRID.active && VGRID.hoverMesh) {
    const snapped = vgridSnap(pickRay);
    if (snapped) {
      VGRID.hoverMesh.position.copy(snapped.worldPos);
      VGRID.hoverMesh.visible = true;
      VGRID.hoverPos = snapped;
      document.getElementById('gh-dist').textContent = `${snapped.distCm.toFixed(1)} cm  H:${snapped.uCm.toFixed(1)} V:${snapped.vCm.toFixed(1)}`;
    } else {
      VGRID.hoverMesh.visible = false; VGRID.hoverPos = null;
      document.getElementById('gh-dist').textContent = 'Survolez…';
    }
  }
}

// ══════════════════════════════════════════════════
// CLICK — Priority: objects/points > grid > ground
// ══════════════════════════════════════════════════
let flashTimer = null;
function showFlash(msg) {
  const el = document.getElementById('pt-flash');
  el.textContent = '✓ ' + msg; el.style.display = 'block';
  if (flashTimer) clearTimeout(flashTimer);
  flashTimer = setTimeout(() => { el.style.display='none'; }, 2500);
}

cv.addEventListener('click', ev => {
  if (isRD) return;
  const r = cv.getBoundingClientRect();
  const m = new THREE.Vector2(((ev.clientX-r.left)/getW())*2-1, -((ev.clientY-r.top)/getH())*2+1);
  pickRay.setFromCamera(m, camera);
  const mode = currentScene ? currentScene.mode : 'object_designer';

  // ── PRIORITY 1: existing objects / points / segments ─────────
  const tgts = [];
  Object.values(objGroups).forEach(g => g.traverse(c => { if (c.userData && c.userData.type) tgts.push(c); }));
  const hits = pickRay.intersectObjects(tgts, false);

  if (hits.length > 0) {
    const ud = hits[0].object.userData;
    if (mode === 'plan_editor') {
      const oid = ud.oid || ud.id;
      vSel = {type:'object', id:oid, oid};
      sendAction({type:'select_object', id:oid});
      document.getElementById('status').textContent = 'Objet #'+oid;
    } else {
      // OD: select point or segment
      vSel = {type:ud.type, id:ud.id, oid:ud.oid};
      buildScene(currentScene); // redraw with highlight
      document.getElementById('status').textContent = (ud.type==='point'?'Point':'Segment') + ' #'+ud.id+' — Suppr=supprimer';
      if (ud.type === 'point') {
        // Activate grid at this point's world position
        const obj = currentScene.objects.find(o => o.id===ud.oid);
        if (obj) {
          const pt = obj.points.find(p => p.id===ud.id);
          if (pt) {
            activateGrid(new THREE.Vector3(
              (obj.pos.x+pt.x)*0.01,
              (obj.pos.y+pt.y)*0.01,
              (obj.pos.z+pt.z)*0.01));
          }
        }
      }
    }
    return; // Do NOT fall through
  }

  // ── PRIORITY 2: grid snap node ────────────────────────────────
  if (VGRID.active && VGRID.hoverPos) {
    const p = VGRID.hoverPos.worldPos;
    sendAction({
      type: mode==='object_designer' ? 'grid_click_od' : 'grid_click_pe',
      x: p.x/0.01, y: p.y/0.01, z: p.z/0.01,
      gridOriginX: VGRID.origin.x/0.01, gridOriginY: VGRID.origin.y/0.01, gridOriginZ: VGRID.origin.z/0.01,
      gridAngle: VGRID.angle, gridCellSize: VGRID.cellSize, gridExtent: VGRID.extent,
    });
    if (mode==='object_designer') {
      showFlash(`(${(p.x/0.01).toFixed(1)}, ${(p.y/0.01).toFixed(1)}, ${(p.z/0.01).toFixed(1)}) cm`);
    }
    return;
  }

  // ── PRIORITY 3: ground → activate grid ───────────────────────
  const gHit = new THREE.Vector3();
  if (pickRay.ray.intersectPlane(gndPl, gHit)) {
    activateGrid(gHit);
    document.getElementById('status').textContent = 'Grille verticale active — survolez puis cliquez';
  }
});

// ── Keyboard ────────────────────────────────────────
window.addEventListener('keydown', ev => {
  if (ev.key === 'Escape') { dismissGrid(); return; }
  const mode = currentScene ? currentScene.mode : 'object_designer';
  if ((ev.key==='Delete'||ev.key==='Backspace') && mode==='object_designer') {
    if (vSel.type==='point') {
      sendAction({type:'delete_point', id:vSel.id,
        gridOriginX:VGRID.origin.x/0.01, gridOriginY:VGRID.origin.y/0.01, gridOriginZ:VGRID.origin.z/0.01,
        gridAngle:VGRID.angle, gridCellSize:VGRID.cellSize, gridExtent:VGRID.extent});
      vSel={type:null,id:null,oid:null};
    } else if (vSel.type==='segment') {
      sendAction({type:'delete_segment', id:vSel.id,
        gridOriginX:VGRID.origin.x/0.01, gridOriginY:VGRID.origin.y/0.01, gridOriginZ:VGRID.origin.z/0.01,
        gridAngle:VGRID.angle, gridCellSize:VGRID.cellSize, gridExtent:VGRID.extent});
      vSel={type:null,id:null,oid:null};
    }
    ev.preventDefault();
  }
});

// ══════════════════════════════════════════════════
// SCENE POLLING — reads from parent DOM every 300ms
// ══════════════════════════════════════════════════
let lastSceneStr = '';
function pollScene() {
  const s = getScene();
  if (!s) return;
  const str = JSON.stringify(s);
  if (str === lastSceneStr) return;
  lastSceneStr = str;
  buildScene(s);
}
setInterval(pollScene, 300);

// ── Render loop ────────────────────────────────────
(function loop() { requestAnimationFrame(loop); renderer.render(threeScene, camera); })();
</script>
</body></html>"""


def render_viewer(height=560):
    st.markdown('<div class="viewer-wrap">', unsafe_allow_html=True)
    st_html(_VIEWER_HTML, height=height+4, scrolling=False)
    st.markdown("</div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# PANELS
# ─────────────────────────────────────────────────────────────────────────────
def _obj_idx(obj_df, oid):
    m=obj_df.index[obj_df["object_id"]==oid]; return m[0] if len(m) else None

def panel_plan_editor(obj_df, pts_df, seg_df, sel_oid, coinc_ids):
    pending=st.session_state.get("pending_place")
    if pending:
        st.markdown(f'<div class="pending-box">📍 Nœud grille : X {pending["x"]:.1f} · Y {pending["y"]:.1f} · Z {pending["z"]:.1f} cm</div>',unsafe_allow_html=True)
        if sel_oid is not None:
            rows=obj_df[obj_df["object_id"]==sel_oid]
            if not rows.empty:
                o=rows.iloc[0]; ax,ay,az=float(o.get("anchor_x",0)),float(o.get("anchor_y",0)),float(o.get("anchor_z",0))
                npx,npy,npz=pending["x"]-ax,pending["y"]-ay,pending["z"]-az
                st.markdown(f'<div class="info-box">→ pos objet : ({npx:.1f},{npy:.1f},{npz:.1f}) cm</div>',unsafe_allow_html=True)
                if st.button("📦 Placer ici",key="do_place"):
                    idx=_obj_idx(obj_df,sel_oid)
                    if idx is not None:
                        df2=obj_df.copy(); df2.at[idx,"pos_x"]=npx;df2.at[idx,"pos_y"]=npy;df2.at[idx,"pos_z"]=npz
                        save_parquet(df2,OBJ_KEY)
                    st.session_state["pending_place"]=None; st.rerun()
        if st.button("❌ Annuler",key="cancel_place"):
            st.session_state["pending_place"]=None; st.rerun()
        st.divider()

    if coinc_ids:
        st.markdown(f'<div class="pending-box" style="background:#1f0d0d;border-color:#5a1a1a;color:#f78166">⚠️ {len(coinc_ids)} points coïncidents</div>',unsafe_allow_html=True)
    if sel_oid is None:
        st.markdown('<div class="info-box">👆 Cliquez un objet dans la vue ou la liste.</div>',unsafe_allow_html=True); return

    rows=obj_df[obj_df["object_id"]==sel_oid]
    if rows.empty: return
    obj=rows.iloc[0]
    px,py,pz=float(obj["pos_x"]),float(obj["pos_y"]),float(obj["pos_z"])
    qx,qy,qz,qw=float(obj["rot_x"]),float(obj["rot_y"]),float(obj["rot_z"]),float(obj["rot_w"])
    ex,ey,ez=quat_to_euler(qx,qy,qz,qw)
    n_p=len(pts_df[pts_df["object_id"]==sel_oid]) if not pts_df.empty else 0
    n_s=len(seg_df[seg_df["object_id"]==sel_oid])  if not seg_df.empty else 0

    st.markdown(f'<div class="metric-row"><div class="metric-card"><div class="metric-val">{obj["name"]}</div><div class="metric-lbl">Objet</div></div><div class="metric-card"><div class="metric-val">{n_p}</div><div class="metric-lbl">Points</div></div><div class="metric-card"><div class="metric-val">{n_s}</div><div class="metric-lbl">Segments</div></div></div>',unsafe_allow_html=True)
    st.markdown(f'<div class="pos-display"><div class="pos-axis"><span>X</span>{px:.1f}</div><div class="pos-axis"><span>Y</span>{py:.1f}</div><div class="pos-axis"><span>Z</span>{pz:.1f}</div><div class="pos-axis" style="margin-left:8px"><span>RY</span>{ey:.1f}°</div></div>',unsafe_allow_html=True)

    tabs=st.tabs(["🕹 Déplacer","🔄 Pivoter","📐 Échelle","⚓ Ancre","📍 Exact","↗ Aligner","🗑"])
    with tabs[0]:
        step=st.number_input("Pas (cm)",0.1,9999.0,st.session_state["move_step"],0.1,"%.1f",key="v_ms")
        st.session_state["move_step"]=step
        def _mv(dx=0,dy=0,dz=0):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is None: return
            df2=obj_df.copy(); df2.at[idx,"pos_x"]+=dx;df2.at[idx,"pos_y"]+=dy;df2.at[idx,"pos_z"]+=dz
            save_parquet(df2,OBJ_KEY); st.rerun()
        st.markdown('<p class="move-lbl">X / Z</p>',unsafe_allow_html=True)
        _,tc,_=st.columns([1,1,1])
        if tc.button("⬆ −Z",key="m_mz",use_container_width=True): _mv(dz=-step)
        l,mc,r=st.columns(3)
        if l.button("◀ −X",key="m_mx",use_container_width=True): _mv(dx=-step)
        mc.markdown(f"<div style='text-align:center;padding:8px 0;border:1px solid #21262d;border-radius:5px;font-size:10px;color:#888'>X{px:.1f}<br>Z{pz:.1f}</div>",unsafe_allow_html=True)
        if r.button("▶ +X",key="m_px",use_container_width=True): _mv(dx=+step)
        _,bc,_=st.columns([1,1,1])
        if bc.button("⬇ +Z",key="m_pz",use_container_width=True): _mv(dz=+step)
        st.markdown('<p class="move-lbl">Y</p>',unsafe_allow_html=True)
        y1,y2,y3=st.columns(3)
        if y1.button("▲ +Y",key="m_py",use_container_width=True): _mv(dy=+step)
        y2.markdown(f"<div style='text-align:center;padding:5px 0;font-size:10px;color:#888'>Y{py:.1f}</div>",unsafe_allow_html=True)
        if y3.button("▼ −Y",key="m_my",use_container_width=True): _mv(dy=-step)

    with tabs[1]:
        rstep=st.number_input("Pas (°)",0.1,180.0,st.session_state["rot_step"],0.5,"%.1f",key="v_rs")
        st.session_state["rot_step"]=rstep
        def _rot(axis,deg):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is None: return
            df2=obj_df.copy()
            nx,ny,nz,nw=compose_rot(float(df2.at[idx,"rot_x"]),float(df2.at[idx,"rot_y"]),float(df2.at[idx,"rot_z"]),float(df2.at[idx,"rot_w"]),axis,deg)
            df2.at[idx,"rot_x"]=nx;df2.at[idx,"rot_y"]=ny;df2.at[idx,"rot_z"]=nz;df2.at[idx,"rot_w"]=nw
            save_parquet(df2,OBJ_KEY); st.rerun()
        for ll,ax in [("Axe Y","y"),("Axe X","x"),("Axe Z","z")]:
            st.markdown(f'<p class="move-lbl">{ll}</p>',unsafe_allow_html=True); c1,c2=st.columns(2)
            if c1.button(f"↺ −{rstep:.1f}°",key=f"r{ax}m",use_container_width=True): _rot(ax,-rstep)
            if c2.button(f"↻ +{rstep:.1f}°",key=f"r{ax}p",use_container_width=True): _rot(ax,+rstep)
        st.markdown(f'<div class="info-box">RX {ex:.1f}° · RY {ey:.1f}° · RZ {ez:.1f}°</div>',unsafe_allow_html=True)
        if st.button("⟲ Reset",key="rot_rst"):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is not None:
                df2=obj_df.copy(); df2.at[idx,"rot_x"]=0;df2.at[idx,"rot_y"]=0;df2.at[idx,"rot_z"]=0;df2.at[idx,"rot_w"]=1
                save_parquet(df2,OBJ_KEY); st.rerun()

    with tabs[2]:
        sstep=st.number_input("Pas",0.01,100.0,st.session_state["scale_step"],0.05,"%.2f",key="v_ss")
        st.session_state["scale_step"]=sstep; unif=st.checkbox("Uniforme",True,key="scl_u")
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

    with tabs[3]:
        ax_=float(obj.get("anchor_x",0)); ay_=float(obj.get("anchor_y",0)); az_=float(obj.get("anchor_z",0))
        st.markdown(f'<div class="info-box">Ancre : ({ax_:.1f},{ay_:.1f},{az_:.1f}) cm</div>',unsafe_allow_html=True)
        o_pts_a=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
        if not o_pts_a.empty:
            pm={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":(float(r["x"]),float(r["y"]),float(r["z"])) for _,r in o_pts_a.iterrows()}
            pm["Origine (0,0,0)"]=(0.,0.,0.)
            ch=st.selectbox("Ancre",list(pm.keys()),key="anch_pick")
            if st.button("Définir",key="set_anch"):
                px2,py2,pz2=pm[ch]; idx=_obj_idx(obj_df,sel_oid)
                if idx is not None:
                    df2=obj_df.copy(); df2.at[idx,"anchor_x"]=px2;df2.at[idx,"anchor_y"]=py2;df2.at[idx,"anchor_z"]=pz2
                    save_parquet(df2,OBJ_KEY); st.success("OK"); st.rerun()

    with tabs[4]:
        c1,c2,c3=st.columns(3)
        npx_=c1.number_input("X",value=px,step=1.0,format="%.1f",key=f"apx{sel_oid}")
        npy_=c2.number_input("Y",value=py,step=1.0,format="%.1f",key=f"apy{sel_oid}")
        npz_=c3.number_input("Z",value=pz,step=1.0,format="%.1f",key=f"apz{sel_oid}")
        if st.button("Appliquer",key="abs_pos"):
            idx=_obj_idx(obj_df,sel_oid)
            if idx is not None:
                df2=obj_df.copy(); df2.at[idx,"pos_x"]=npx_;df2.at[idx,"pos_y"]=npy_;df2.at[idx,"pos_z"]=npz_
                save_parquet(df2,OBJ_KEY); st.rerun()

    with tabs[5]:
        o_pts2=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
        if o_pts2.empty: st.info("Ajoutez des points d'abord.")
        else:
            pm3={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":(float(r["x"]),float(r["y"]),float(r["z"])) for _,r in o_pts2.iterrows()}
            ref=pm3[st.selectbox("Réf.",list(pm3.keys()),key="aref")]
            c1,c2,c3=st.columns(3)
            tx=c1.number_input("X",value=px,step=1.0,format="%.1f",key=f"tx{sel_oid}")
            ty=c2.number_input("Y",value=py,step=1.0,format="%.1f",key=f"ty{sel_oid}")
            tz=c3.number_input("Z",value=pz,step=1.0,format="%.1f",key=f"tz{sel_oid}")
            if st.button("↗ Aligner",key="do_align",use_container_width=True):
                idx=_obj_idx(obj_df,sel_oid)
                if idx is not None:
                    df2=obj_df.copy(); df2.at[idx,"pos_x"]=tx-ref[0];df2.at[idx,"pos_y"]=ty-ref[1];df2.at[idx,"pos_z"]=tz-ref[2]
                    save_parquet(df2,OBJ_KEY); st.rerun()

    with tabs[6]:
        st.warning(f"Supprimer {obj['name']} ?")
        if st.button("🗑 Confirmer",key="del_obj_c"):
            for d_,k_ in [(obj_df[obj_df["object_id"]!=sel_oid],OBJ_KEY),
                          (pts_df[pts_df["object_id"]!=sel_oid] if not pts_df.empty else pts_df,PTS_KEY),
                          (seg_df[seg_df["object_id"]!=sel_oid] if not seg_df.empty else seg_df,SEG_KEY)]:
                save_parquet(d_,k_)
            st.session_state["object_id"]=None; st.rerun()


def panel_object_designer(obj_df, pts_df, seg_df, sel_oid):
    msg=st.session_state.get("_last_pt_msg","")
    if msg:
        st.markdown(f'<div class="success-flash">✓ Point créé : {msg}</div>',unsafe_allow_html=True)
        st.session_state["_last_pt_msg"]=""

    if sel_oid is None:
        st.markdown('<div class="info-box">👆 Sélectionnez un objet.<br><br>'
                    '🖱 <b>Clic sol</b> → grille verticale<br>'
                    '🖱 <b>Clic point existant</b> → sélectionner + grille au point<br>'
                    '🖱 <b>Survol grille + clic</b> → créer point (grille reste active)<br>'
                    '⌨ <b>Suppr</b> → supprimer · <b>Échap</b> → fermer grille</div>',
                    unsafe_allow_html=True); return

    if obj_df[obj_df["object_id"]==sel_oid].empty: return
    o_pts=pts_df[pts_df["object_id"]==sel_oid] if not pts_df.empty else pd.DataFrame()
    o_segs=seg_df[seg_df["object_id"]==sel_oid]  if not seg_df.empty else pd.DataFrame()

    tab_pts,tab_segs,tab_csv=st.tabs(["📍 Points","🔗 Segments","⬇ CSV"])

    with tab_pts:
        with st.expander("➕ Ajouter manuellement",expanded=o_pts.empty):
            c1,c2,c3,c4=st.columns([2,2,2,1])
            nx=c1.number_input("X",0.0,step=0.1,format="%.1f",key="np_x")
            ny=c2.number_input("Y",0.0,step=0.1,format="%.1f",key="np_y")
            nz=c3.number_input("Z",0.0,step=0.1,format="%.1f",key="np_z")
            c4.markdown("<br>",unsafe_allow_html=True)
            if c4.button("OK",key="add_pt"):
                pid=next_id(pts_df,"point_id")
                save_parquet(pd.concat([pts_df,pd.DataFrame([{"point_id":pid,"object_id":sel_oid,"x":float(nx),"y":float(ny),"z":float(nz)}])],ignore_index=True),PTS_KEY); st.rerun()

        if o_pts.empty:
            st.info("Aucun point. Cliquez la grille ou ajoutez manuellement."); return

        pt_map={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":int(r["point_id"]) for _,r in o_pts.iterrows()}
        sel_lbl=st.selectbox("Point actif",list(pt_map.keys()),key="sel_pt_lbl")
        sel_pid=pt_map[sel_lbl]; pt_row=o_pts[o_pts["point_id"]==sel_pid].iloc[0]
        cx,cy,cz=float(pt_row["x"]),float(pt_row["y"]),float(pt_row["z"])
        st.markdown(f'<div class="pos-display"><div class="pos-axis"><span>X</span>{cx:.1f}</div><div class="pos-axis"><span>Y</span>{cy:.1f}</div><div class="pos-axis"><span>Z</span>{cz:.1f}</div></div>',unsafe_allow_html=True)

        pstep=st.number_input("Pas (cm)",0.1,9999.0,st.session_state["pt_move_step"],0.1,"%.1f",key="pt_step")
        st.session_state["pt_move_step"]=pstep

        def _mpt(dx=0,dy=0,dz=0):
            idx=pts_df.index[pts_df["point_id"]==sel_pid][0]
            df2=pts_df.copy(); df2.at[idx,"x"]+=dx;df2.at[idx,"y"]+=dy;df2.at[idx,"z"]+=dz
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
                    if len(idx): df2.at[idx[0],"x"]=float(rr["x"]);df2.at[idx[0],"y"]=float(rr["y"]);df2.at[idx[0],"z"]=float(rr["z"])
            save_parquet(df2,PTS_KEY); st.success("OK"); st.rerun()
        if c2.button("🗑 Supprimer",key="del_pt"):
            p2=pts_df[pts_df["point_id"]!=sel_pid]
            s2=seg_df[(seg_df["point_a_id"]!=sel_pid)&(seg_df["point_b_id"]!=sel_pid)] if not seg_df.empty else seg_df
            save_parquet(p2,PTS_KEY); save_parquet(s2,SEG_KEY); st.rerun()

    with tab_segs:
        if o_pts.empty or len(o_pts)<2: st.info("≥2 points requis."); return
        pt_lbl2={f"#{int(r['point_id'])} ({float(r['x']):.1f},{float(r['y']):.1f},{float(r['z']):.1f})":int(r["point_id"]) for _,r in o_pts.iterrows()}
        lbls=list(pt_lbl2.keys()); c1,c2=st.columns(2)
        sa=c1.selectbox("A",lbls,key="seg_a"); sb=c2.selectbox("B",lbls,key="seg_b",index=min(1,len(lbls)-1))
        if st.button("🔗 Créer segment",key="mk_seg"):
            pa_id,pb_id=pt_lbl2[sa],pt_lbl2[sb]
            if pa_id==pb_id: st.error("Deux points distincts requis.")
            else:
                dupe=(not seg_df.empty) and not seg_df[(seg_df["object_id"]==sel_oid)&(((seg_df["point_a_id"]==pa_id)&(seg_df["point_b_id"]==pb_id))|((seg_df["point_a_id"]==pb_id)&(seg_df["point_b_id"]==pa_id)))].empty
                if dupe: st.warning("Existe déjà.")
                else:
                    sid=next_id(seg_df,"segment_id")
                    save_parquet(pd.concat([seg_df,pd.DataFrame([{"segment_id":sid,"object_id":sel_oid,"point_a_id":pa_id,"point_b_id":pb_id}])],ignore_index=True),SEG_KEY); st.rerun()
        if not o_segs.empty:
            st.markdown(f"**{len(o_segs)} segment(s)**")
            st.dataframe(o_segs[["segment_id","point_a_id","point_b_id"]].reset_index(drop=True),use_container_width=True,hide_index=True)
            c1,c2=st.columns([3,1]); dsid=c1.selectbox("Suppr",o_segs["segment_id"].tolist(),key="dseg")
            if c2.button("🗑",key="dseg_b"): save_parquet(seg_df[seg_df["segment_id"]!=dsid],SEG_KEY); st.rerun()

    with tab_csv:
        st.markdown('<div class="info-box">Format : <code>x,y,z</code> par ligne (cm)</div>',unsafe_allow_html=True)
        up=st.file_uploader("CSV",type=["csv"],key="csv_up")
        if up:
            try:
                dfc=pd.read_csv(up,names=["x","y","z"]); st.dataframe(dfc.head(10),use_container_width=True)
                if st.button("⬇ Importer",key="do_import"):
                    base=next_id(pts_df,"point_id")
                    new=[{"point_id":base+i,"object_id":sel_oid,"x":float(rr["x"]),"y":float(rr["y"]),"z":float(rr["z"])} for i,(_,rr) in enumerate(dfc.iterrows())]
                    save_parquet(pd.concat([pts_df,pd.DataFrame(new)],ignore_index=True),PTS_KEY); st.success(f"{len(new)} pts"); st.rerun()
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

    # ── ACTION BUS: viewer → Python ───────────────────────────────────
    # Uses aria-label to be findable by JS without :has() CSS
    st.markdown('<div style="position:absolute;opacity:0;pointer-events:none;width:1px;height:1px;overflow:hidden">',unsafe_allow_html=True)
    viewer_msg=st.text_input("__action_bus__",key="_viewer_msg",label_visibility="hidden")
    st.markdown('</div>',unsafe_allow_html=True)

    if viewer_msg and viewer_msg not in ("","{}"):
        try:
            parsed=json.loads(viewer_msg)
            if parsed.get("type"):
                process_viewer_action(viewer_msg,obj_df,pts_df,seg_df)
        except: pass
        st.session_state["_viewer_msg"]=""

    # ── Sync grid when object selection changes ────────────────────────
    cur_oid=st.session_state.get("object_id")
    if cur_oid!=st.session_state.get("_prev_oid"):
        st.session_state["_prev_oid"]=cur_oid
        if cur_oid is not None: sync_grid_from_object(obj_df,cur_oid)

    cur_pid=st.session_state.get("project_id")
    coinc=set()
    if st.session_state["mode"]=="plan_editor" and not obj_df.empty and not pts_df.empty:
        coinc=find_coincident(obj_df,pts_df)

    scene=build_scene_json(cur_pid,obj_df,pts_df,seg_df,cur_oid,
                           st.session_state.get("selected_pts",[]),coinc)
    scene_str=json.dumps(scene, separators=(',',':'))

    # ── SCENE BUS: Python → viewer via data attribute on a div ────────
    # This div is found by the viewer's getScene() function
    st.markdown(
        f'<div data-scene-json=\'{scene_str}\' style="display:none"></div>',
        unsafe_allow_html=True)

    # ── SIDEBAR ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<div class="studio-header"><div><div class="studio-title">🧊 3D Design Studio</div><div class="studio-sub">Point · Segment · Transform</div></div></div>',unsafe_allow_html=True)
        mode_lbl=st.radio("Mode",["📐 Plan Editor","✏️ Object Designer"],
            index=0 if st.session_state["mode"]=="plan_editor" else 1,
            horizontal=True,label_visibility="collapsed")
        st.session_state["mode"]="plan_editor" if "Plan" in mode_lbl else "object_designer"
        bcls2="badge-plan" if st.session_state["mode"]=="plan_editor" else "badge-object"
        blbl2="PLAN EDITOR" if st.session_state["mode"]=="plan_editor" else "OBJECT DESIGNER"
        st.markdown(f'<span class="badge {bcls2}">{blbl2}</span>',unsafe_allow_html=True)
        st.divider()

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
            cp=st.session_state["project_id"]; ci=pids.index(cp) if cp in pids else 0
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

        st.markdown('<p class="section-label">📦 Objets</p>',unsafe_allow_html=True)
        if cur_pid is not None:
            with st.expander("Nouvel objet"):
                oname=st.text_input("Nom",key="new_obj_name",placeholder="Objet A…")
                if st.button("Créer",key="create_obj"):
                    oid=next_id(obj_df,"object_id")
                    obj_df=pd.concat([obj_df,pd.DataFrame([{"object_id":oid,"project_id":cur_pid,
                        "name":oname.strip() or f"Objet {oid}",
                        "pos_x":0.,"pos_y":0.,"pos_z":0.,"rot_x":0.,"rot_y":0.,"rot_z":0.,"rot_w":1.,
                        "scale_x":1.,"scale_y":1.,"scale_z":1.,"anchor_x":0.,"anchor_y":0.,"anchor_z":0.,
                        "grid_cell_size":float(st.session_state["grid_cell_size"]),
                        "grid_extent":int(st.session_state["grid_extent"]),
                        "grid_angle":int(st.session_state["grid_angle"]),
                    }])],ignore_index=True)
                    save_parquet(obj_df,OBJ_KEY); st.session_state["object_id"]=oid; st.rerun()
            proj_objs=obj_df[obj_df["project_id"]==cur_pid] if not obj_df.empty else pd.DataFrame()
            sel_oid2=st.session_state.get("object_id")
            if not proj_objs.empty:
                for _,o in proj_objs.iterrows():
                    oid2=int(o["object_id"]); active=oid2==sel_oid2
                    np_=len(pts_df[pts_df["object_id"]==oid2]) if not pts_df.empty else 0
                    if st.button(f"{'▶ ' if active else '  '}{o['name']} · {np_}pt",key=f"sel_{oid2}",use_container_width=True):
                        st.session_state["object_id"]=oid2; st.rerun()
            else: st.caption("Aucun objet.")
        else: st.caption("Sélectionnez un projet.")
        st.divider()

        st.markdown('<p class="section-label">👁 Affichage</p>',unsafe_allow_html=True)
        c1,c2=st.columns(2)
        st.session_state["show_grid"]=c1.checkbox("Grille fond",value=True)
        st.session_state["show_axes"]=c2.checkbox("Axes",value=True)
        st.session_state["snap"]=st.checkbox("Snap visuel",value=True)
        if st.session_state["snap"]:
            st.session_state["snap_dist"]=st.slider("Seuil snap",0.5,30.0,5.0,0.5,label_visibility="collapsed")

    # ── VIEWER (stable, never changes) ────────────────────────────────
    render_viewer(height=560)

    st.markdown("<hr style='border-color:#21262d;margin:6px 0'>",unsafe_allow_html=True)

    if st.session_state["mode"]=="plan_editor":
        panel_plan_editor(obj_df,pts_df,seg_df,cur_oid,coinc)
    else:
        panel_object_designer(obj_df,pts_df,seg_df,cur_oid)


if __name__=="__main__":
    main()
