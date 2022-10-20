# List of instance ids to bring to process
# Note that the version is for now ignored
# (the latest is always chosen) TODO: See if
# we can make this specific to the version
import asyncio

from pangeo_forge_esgf import generate_recipe_inputs_from_iids

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

iids = [
    # PMIP runs requested by @CommonClimate
    'CMIP6.PMIP.MIROC.MIROC-ES2L.past1000.r1i1p1f2.Amon.tas.gn.v20200318',
    'CMIP6.PMIP.MRI.MRI-ESM2-0.past1000.r1i1p1f1.Amon.tas.gn.v20200120',
    'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.past2k.r1i1p1f1.Amon.tas.gn.v20210714',
        #PMIP velocities
     'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gn.v20191002',
  #   'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.uo.gn.v20200212', #This one did fail with a pangeo-forge-esgf issue (in a local test) that I need to fix over there.
     'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200212',
     'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.uo.gr1.v20200911',
     'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.uo.gn.v20200909',
     'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Omon.vo.gn.v20200212',
     'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gn.v20191002',
  #   'CMIP6.PMIP.AWI.AWI-ESM-1-1-LR.lgm.r1i1p1f1.Odec.vo.gn.v20200212', #same
     'CMIP6.PMIP.MIROC.MIROC-ES2L.lgm.r1i1p1f2.Omon.vo.gr1.v20200911',
     'CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.lgm.r1i1p1f1.Omon.vo.gn.v20190710',
]

recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(iids))

recipes = {}

for iid, recipe_input in recipe_inputs.items():
    urls = recipe_input.get('urls', None)
    pattern_kwargs = recipe_input.get('pattern_kwargs', {})
    recipe_kwargs = recipe_input.get('recipe_kwargs', {})

    pattern = pattern_from_file_sequence(urls, 'time', **pattern_kwargs)
    if urls is not None:
        recipes[iid] = XarrayZarrRecipe(
            pattern, xarray_concat_kwargs={'join': 'exact'}, **recipe_kwargs
        )
print('+++Failed iids+++')
print(list(set(iids) - set(recipes.keys())))
print('+++Successful iids+++')
print(list(recipes.keys()))
