# Keeping the paper in sync with Overleaf

The paper is the `paper/` directory of the duckpgq repository; the Overleaf project holds that directory as its
root (git subtree). Overleaf's remote is `overleaf`, branch `main`.

- Bring Overleaf edits here (do this before pushing):
  `git subtree pull --prefix=paper overleaf main --squash -m "Overleaf edits"`
- Send local commits to Overleaf:
  `git subtree push --prefix=paper overleaf main`

Review marks (`\rev{...}`, green) are off by default so Overleaf prints plain text. Locally they are switched on at
build time:
  `cd paper && latexmk -pdf -usepretex='\AtBeginDocument{\reviewmarkstrue}' main.tex`
