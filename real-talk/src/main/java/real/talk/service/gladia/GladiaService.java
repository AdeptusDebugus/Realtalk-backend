package real.talk.service.gladia;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import real.talk.model.entity.GladiaData;
import real.talk.model.entity.enums.DataStatus;
import real.talk.repository.gladia.GladiaDataRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class GladiaService {

    private final GladiaDataRepository gladiaDataRepository;

    public List<GladiaData> getGladiaDataByStatusCreated(){
        return gladiaDataRepository.findGladiaDataByStatus(DataStatus.CREATED);
    }

    public Optional<GladiaData> getGladiaDataByLessonIdAndStatusDone(UUID lessonId){
        return gladiaDataRepository.findGladiaDataByLessonIdAndStatus(lessonId, DataStatus.DONE);
    }

    public GladiaData saveGladiaData(GladiaData gladiaData){
        return gladiaDataRepository.save(gladiaData);
    }
}
