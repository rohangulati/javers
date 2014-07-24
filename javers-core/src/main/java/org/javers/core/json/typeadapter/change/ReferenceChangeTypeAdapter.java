package org.javers.core.json.typeadapter.change;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import org.javers.core.commit.CommitMetadata;
import org.javers.core.diff.changetype.ReferenceChange;
import org.javers.core.metamodel.object.GlobalCdoId;

public class ReferenceChangeTypeAdapter extends ChangeTypeAdapter<ReferenceChange> {

    private static final String LEFT_REFERENCE_FIELD = "left";
    private static final String RIGHT_REFERENCE_FIELD = "right";

    @Override
    public ReferenceChange fromJson(JsonElement json, JsonDeserializationContext context) {
        JsonObject jsonObject = (JsonObject) json;
        PropertyChangeStub stub = deserializeStub(jsonObject, context);

        GlobalCdoId leftRef  = context.deserialize(jsonObject.get(LEFT_REFERENCE_FIELD),  GlobalCdoId.class);
        GlobalCdoId rightRef = context.deserialize(jsonObject.get(RIGHT_REFERENCE_FIELD), GlobalCdoId.class);

        return appendCommitMetadata(jsonObject, context, new ReferenceChange(stub.id, stub.property, leftRef, rightRef));
    }

    @Override
    public JsonElement toJson(ReferenceChange change, JsonSerializationContext context) {
        final JsonObject jsonObject = createJsonObject(change, context);

        jsonObject.add(LEFT_REFERENCE_FIELD,  context.serialize(change.getLeft()));
        jsonObject.add(RIGHT_REFERENCE_FIELD, context.serialize(change.getRight()));

        return jsonObject;
    }

    @Override
    public Class getValueType() {
        return ReferenceChange.class;
    }
}