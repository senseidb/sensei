package com.senseidb.search.relevance;

import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.query.ScoreAugmentQuery.ScoreAugmentFunction;
import com.senseidb.search.relevance.CustomRelevanceFunction.CustomRelevanceFunctionFactory;
import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;
import com.senseidb.search.relevance.impl.CompilationHelper;
import com.senseidb.search.relevance.impl.CompilationHelper.DataTable;
import com.senseidb.search.relevance.impl.CustomMathModel;
import com.senseidb.search.relevance.impl.RelevanceException;
import com.senseidb.search.relevance.impl.RelevanceJSONConstants;
import com.senseidb.search.req.ErrorType;

public class RelevanceFunctionBuilder {

  public static ScoreAugmentFunction build(JSONObject jsonRelevance) throws JSONException {
    // first handle the predefined case if there is any one existing in the json;
    if (jsonRelevance.has(RelevanceJSONConstants.KW_PREDEFINED)) {
      String modelName = jsonRelevance.getString(RelevanceJSONConstants.KW_PREDEFINED);

      if (ModelStorage.hasPreloadedModel(modelName)) {
        CustomRelevanceFunctionFactory crfFactory = ModelStorage.getPreloadedModel(modelName);
        return crfFactory.build();
      } else if (ModelStorage.hasRuntimeModel(modelName)) {
        RuntimeRelevanceFunctionFactory rrfFactory = ModelStorage.getRuntimeModel(modelName);
        return rrfFactory.build();
      } else {
        throw new JSONException(
            "No such model (CustomRelevanceFunctionFactory plugin) is registered: " + modelName);
      }
    }

    // runtime anonymous model;
    else if (jsonRelevance.has(RelevanceJSONConstants.KW_MODEL)) {
      JSONObject modelJson = jsonRelevance.optJSONObject(RelevanceJSONConstants.KW_MODEL);

      // build the model factory and one model;
      // if the model factory needs to be saved, the factory will be used, otherwise, just return
      // the model object (RuntimeRelevanceFunction);
      RuntimeRelevanceFunctionFactory rrfFactory = (RuntimeRelevanceFunctionFactory) buildModelFactoryFromModelJSON(modelJson);
      RuntimeRelevanceFunction sm = (RuntimeRelevanceFunction) rrfFactory.build();

      // store the model if specified;
      if (modelJson.has(RelevanceJSONConstants.KW_SAVE_AS)) {
        // "save_as":{
        // "name":"RuntimeModelName",
        // "overwrite":true
        // }
        JSONObject jsonSaveAS = modelJson.getJSONObject(RelevanceJSONConstants.KW_SAVE_AS);
        String newRuntimeName = jsonSaveAS.getString(RelevanceJSONConstants.KW_NAME_AS);
        boolean overwrite = false;
        if (jsonSaveAS.has(RelevanceJSONConstants.KW_OVERWRITE)) overwrite = jsonSaveAS
            .getBoolean(RelevanceJSONConstants.KW_OVERWRITE);

        if ((ModelStorage.hasRuntimeModel(newRuntimeName) || ModelStorage
            .hasPreloadedModel(newRuntimeName)) && (overwrite == false)) throw new IllegalArgumentException(
            "the runtime model name "
                + newRuntimeName
                + " already exists, or you did not ask to overwrite the old model. Set \"overwrite\":true in the json will replace the old model if you want.");

        ModelStorage.injectRuntimeModel(newRuntimeName, rrfFactory);
      }

      return sm;
    } else {
      throw new IllegalArgumentException("the relevance json is not valid");
    }
  }

  /**
   * A facility method to build a relevance model factory object from a relevance json object;
   * A relevance model factory can be used to generate scorefunction object as many as you want.
   * @param jsonRelevance
   * @return Relevance model Factory
   * @throws RelevanceException this could be a wrapper of JSONException, it may also provide some compilation error message.
   * Use {@link RelevanceException#getErrorCode()} method to get the error code for this exception.
   * If the code is equal to {@link ErrorType#JsonCompilationError}, {@link RelevanceException#getMessage()} will give a basic error message,
   * while {@link RelevanceException#getCause()} will give the internal error message from JAVASSIST (more useful).
   * If the error code is equal to {@link ErrorType#JsonParsingError}, then there could be some JSON format mistake there (not compilation error).
   */
  public static CustomRelevanceFunctionFactory buildModelFactoryFromModelJSON(JSONObject modelJson)
      throws RelevanceException {
    // runtime anonymous model;
    try {
      DataTable _dt = new DataTable();
      CustomMathModel _cModel = CompilationHelper.createCustomMathScorer(modelJson, _dt);
      RuntimeRelevanceFunction sm = new RuntimeRelevanceFunction(_cModel, _dt);
      RuntimeRelevanceFunctionFactory rrfFactory = new RuntimeRelevanceFunctionFactory(sm);
      return rrfFactory;
    } catch (JSONException e) {
      if (e instanceof RelevanceException) {
        throw (RelevanceException) e;
      } else throw new RelevanceException(ErrorType.JsonParsingError,
          "Json format is not correct.", e);
    }
  }
}
